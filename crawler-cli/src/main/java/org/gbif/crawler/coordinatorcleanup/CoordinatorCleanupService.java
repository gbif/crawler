/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.coordinatorcleanup;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.ws.client.DatasetProcessClient;
import org.gbif.registry.ws.client.DatasetProcessStatusClient;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.util.concurrent.AbstractScheduledService;

/** This services starts the Crawler Coordinator by listening for messages. */
@SuppressWarnings("UnstableApiUsage")
public class CoordinatorCleanupService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorCleanupService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final CoordinatorCleanupConfiguration configuration;
  private CuratorFramework curator;
  private DatasetProcessService service;
  private DatasetProcessStatusService registryService;

  public CoordinatorCleanupService(CoordinatorCleanupConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void runOneIteration() {
    try {
      runOnce();
    } catch (Exception e) {
      LOG.error(
          "Unexpected exception running loop to clean ZK cause by [{}].  Restarting curator, and continuing...",
          e.getMessage(),
          e);
      try {
        initializeCurator();
      } catch (Exception e2) {
        LOG.error("Unexpected exception initializing curator.  Continuing...", e);
      }
    }
  }

  private void runOnce() {
    LOG.info("Checking if any crawls have finished now.");
    Set<DatasetProcessStatus> statuses;
    try {
      statuses = service.getRunningDatasetProcesses();
      LOG.debug("All DatasetProcessStatuses retrieved: {}", statuses);
    } catch (ServiceUnavailableException e) {
      LOG.warn("Caught exception while trying to retrieve all running datasets, will try again", e);
      return;
    }

    for (DatasetProcessStatus status : statuses) {
      try (MDC.MDCCloseable ignored1 =
              MDC.putCloseable("datasetKey", status.getDatasetKey().toString());
          MDC.MDCCloseable ignored2 =
              MDC.putCloseable("attempt", String.valueOf(status.getCrawlJob().getAttempt()))) {
        LOG.info("Checking DatasetProcessStatus with UUID [{}] now", status.getDatasetKey());

        try {
          updateRegistry(status);

          if (!checkDoneProcessing(status)) {
            continue;
          }

          // If all of these things are true we can delete this dataset from ZK and dump info to
          // disc
          updateRegistry(status);
          delete(status);
        } catch (Exception e) {
          LOG.error(
              "Unable to callback and update the registry. Aborting this cleanup round, will try again later.",
              e);
          return;
        }
      }
    }
    LOG.info("Done checking for finished crawls");
  }

  @Override
  protected void startUp() throws Exception {
    service = configuration.registry.newClientBuilder().build(DatasetProcessClient.class);
    registryService = configuration.registry.newClientBuilder().build(DatasetProcessStatusClient.class);
    curator = initializeCurator();
    MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
  }

  /**
   * Initializes the curator framework, attempting to close it first if it is already established.
   * If Zookeeper connectivity is disrupted for example, curator needs reconfigured.
   */
  private CuratorFramework initializeCurator() throws IOException {
    if (curator != null) {
      try {
        curator.close();
      } catch (Exception e) {
        LOG.debug("Unexpected exception while closing curator: {}", e.getMessage());
      }
    }
    curator = configuration.zooKeeper.getCuratorFramework();
    return curator;
  }

  @Override
  protected void shutDown() {
    LOG.info("Shutting down");
    if (curator != null) {
      curator.close();
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, configuration.interval, TimeUnit.MINUTES);
  }

  /**
   * Updates the registry with up to date information about the status of processing this dataset.
   *
   * @throws Exception when there was an error updating the registry. This can happen when it's
   *     offline.
   */
  private void updateRegistry(DatasetProcessStatus status) throws Exception {
    UUID datasetKey = status.getDatasetKey();
    int attempt = status.getCrawlJob().getAttempt();
    DatasetProcessStatus persisted = registryService.getDatasetProcessStatus(datasetKey, attempt);
    LOG.debug(MAPPER.writeValueAsString(status));
    if (persisted == null) {
      registryService.createDatasetProcessStatus(status);
    } else {
      registryService.updateDatasetProcessStatus(status);
    }
  }

  /**
   * This method checks whether a certain Dataset has been fully processed (crawled, occurrences
   * processed etc.)
   *
   * @param status of the dataset in question to check
   * @return {@code true} if we are done processing, {@code false} otherwise
   */
  private boolean checkDoneProcessing(DatasetProcessStatus status) {
    // crawl finished?
    if (status.getFinishedCrawling() == null) {
      LOG.debug("Waiting for crawling to finish.");
      return false;
    }

    // if metadata only, these are set to empty by the metasyncer
    if (ProcessState.EMPTY == status.getProcessStateOccurrence()
        && ProcessState.EMPTY == status.getProcessStateChecklist()
        && ProcessState.EMPTY == status.getProcessStateSample()) {
      LOG.debug("DONE: Empty dataset is finished.");
      return true;
    }

    // no processing started yet
    if (status.getProcessStateChecklist() == null
        && status.getProcessStateOccurrence() == null
        && status.getProcessStateSample() == null) {
      LOG.debug("Waiting for validation to start.");
      return false;
    }

    // checklist indexing running?
    if (status.getProcessStateChecklist() != null
        && status.getProcessStateChecklist() == ProcessState.RUNNING) {
      LOG.debug("Waiting for checklist processing to finish.");
      return false;
    }

    // occurrence (pipeline) indexing running?
    if (status.getProcessStateOccurrence() != null
        && status.getProcessStateOccurrence() == ProcessState.RUNNING) {
      LOG.debug("Waiting for occurrence processing to finish.");
      return false;
    }

    // the crawl is finally done!
    LOG.debug(
        "DONE: crawling, checklist and occurrence processing is completed. (Metadata sync is/was independent.)");

    // Set the sample processing to completed.
    if (status.getProcessStateSample() == ProcessState.RUNNING) {
      status.setProcessStateSample(ProcessState.FINISHED);
    }

    return true;
  }

  private void delete(DatasetProcessStatus status) {
    String statusString;
    try {
      statusString = MAPPER.writeValueAsString(status);
    } catch (IOException e) {
      LOG.warn("Failed to serialize processing status for [{}].", status.getDatasetKey());
      statusString = "failed to serialize";
    }

    LOG.info("Done with [{}]. Status: {}", status.getDatasetKey(), statusString);

    try {
      // This will retry since we provide guaranteed() which could potentially cause issues on race
      // conditions.
      // However, this is seen as highly unlikely, and a cleaned ZK will be operationally easier to
      // manage then
      // the alternative.  Any failed crawls due to some unlikely race condition (which includes a
      // failure to delete)
      // will be picked up quickly in a scheduled recrawl anyway.
      LOG.debug("Deleting ZK path {}", CrawlerNodePaths.getCrawlInfoPath(status.getDatasetKey()));
      curator
          .delete()
          .guaranteed()
          .deletingChildrenIfNeeded()
          .forPath(CrawlerNodePaths.getCrawlInfoPath(status.getDatasetKey()));
    } catch (Exception e) {
      LOG.error(
          "Couldn't delete [{}] - note that a background thread will retry this",
          status.getDatasetKey(),
          e);
    }
  }
}
