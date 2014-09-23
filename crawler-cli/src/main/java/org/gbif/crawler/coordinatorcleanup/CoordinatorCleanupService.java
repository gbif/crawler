/*
 * Copyright 2013 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.gbif.cli.ConfigUtils;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.ws.client.guice.CrawlerWsClientModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.curator.framework.CuratorFramework;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This services starts the Crawler Coordinator by listening for messages.
 */
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
  protected void runOneIteration() throws Exception {
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
      LOG.info("Checking DatasetProcessStatus with UUID [{}] now", status.getDatasetKey());

      try {
        updateRegistry(status);
      } catch (Exception e) {
        LOG.error("Unable to callback and update the registry. Aborting this cleanup round, will try again later.", e);
        return;
      }

      if (!checkDoneProcessing(status)) {
        continue;
      }

      LOG.debug(MAPPER.writeValueAsString(status));
      try {
        File file = new File(configuration.archiveDirectory,
          status.getDatasetKey() + "_" + status.getCrawlJob().getAttempt() + "_result.json");
        Files.createParentDirs(file);
        Files.write(MAPPER.writeValueAsBytes(status), file);
      } catch (IOException e) {
        LOG.warn("Could not write status due to exception. [{}]", status.getDatasetKey(), e);
      }

      // If all of these things are true we can delete this dataset from ZK and dump info to disc
      delete(status.getDatasetKey());
    }
    LOG.info("Done checking for finished crawls");
  }

  @Override
  protected void startUp() throws Exception {
    Properties props = new Properties();
    props.setProperty("registry.ws.url", configuration.registry.wsUrl);

    Injector injector = Guice.createInjector(new CrawlerWsClientModule(ConfigUtils.toProperties(configuration)),
      new RegistryWsClientModule(props),
      new SingleUserAuthModule(configuration.registry.user, configuration.registry.password));
    service = injector.getInstance(DatasetProcessService.class);
    registryService = injector.getInstance(DatasetProcessStatusService.class);
    curator = configuration.zooKeeper.getCuratorFramework();
    MAPPER.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
  }

  @Override
  protected void shutDown() throws Exception {
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
   * @throws Exception when there was an error updating the registry. This can happen when it's offline.
   */
  private void updateRegistry(DatasetProcessStatus status) throws Exception {
    UUID datasetKey = status.getDatasetKey();
    int attempt = status.getCrawlJob().getAttempt();
    DatasetProcessStatus persisted = registryService.getDatasetProcessStatus(datasetKey, attempt);
    LOG.info(MAPPER.writeValueAsString(status));
    if (persisted == null) {
      registryService.createDatasetProcessStatus(status);
    } else {
      registryService.updateDatasetProcessStatus(status);
    }
  }

  /**
   * This method checks whether a certain Dataset has been fully processed (crawled, occurrences processed etc.)
   *
   * @param status of the dataset in question to check
   *
   * @return {@code true} if we are done processing, {@code false} otherwise
   */
  private boolean checkDoneProcessing(DatasetProcessStatus status) {
    // crawl finished?
    if (status.getFinishedCrawling() == null) {
      return false;
    }

    // checklist indexing running?
    if (status.getProcessStateChecklist() != null && status.getProcessStateChecklist() == ProcessState.RUNNING) {
      return false;
    }

    // occurrence processing done?
    if (status.getProcessStateOccurrence() != null && (status.getProcessStateOccurrence() == ProcessState.EMPTY
                                                       || status.getProcessStateOccurrence()
                                                          == ProcessState.FINISHED)) {
      return true;
    }

    // Done fragmenting?
    // We are done when we have as many pages fragmented (in error or successful) as we did crawl
    if (status.getPagesCrawled() != status.getPagesFragmentedError() + status.getPagesFragmentedSuccessful()) {
      return false;
    }

    // Done persisting fragments?
    // We are done when we have processed as many fragments as the fragmenter emitted. During this processing we could
    // generate more raw occurrence records than we got fragments due to ABCD2
    if (status.getFragmentsProcessed() != status.getFragmentsEmitted()) {
      return false;
    }

    // Are we done persisting verbatim occurrences?
    // We are done when we have persisted (in error or successful) as many verbatim records as there were new or
    // updated raw occurrences in the previous steps
    if (status.getVerbatimOccurrencesPersistedSuccessful() + status.getVerbatimOccurrencesPersistedError()
        != status.getRawOccurrencesPersistedNew() + status.getRawOccurrencesPersistedUpdated()) {
      return false;
    }

    // Are we done interpreting occurrences?
    // We are done when we have interpreted (in error or successful) as many occurrences as there were successful
    // verbatim occurrences persisted
    if (status.getInterpretedOccurrencesPersistedSuccessful() + status.getInterpretedOccurrencesPersistedError()
        != status.getVerbatimOccurrencesPersistedSuccessful()) {
      return false;
    }

    return true;
  }

  private void delete(UUID datasetKey) {
    LOG.info("Done with [{}]", datasetKey);
    String path = CrawlerNodePaths.getCrawlInfoPath(datasetKey);
    try {
      recursiveDelete(path, curator.getChildren().forPath(path));
      curator.delete().forPath("crawls/" + datasetKey);
    } catch (Exception e) {
      LOG.info("Couldn't delete [{}]", datasetKey, e);
    }
  }

  private void recursiveDelete(String parentPath, Collection<String> paths) throws Exception {
    if (paths.isEmpty()) {
      return;
    }

    for (String path : paths) {
      String fullPath = parentPath + "/" + path;
      recursiveDelete(fullPath, curator.getChildren().forPath(fullPath));
      curator.delete().forPath(fullPath);
    }
  }

}
