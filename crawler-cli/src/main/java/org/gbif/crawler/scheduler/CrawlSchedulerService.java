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
package org.gbif.crawler.scheduler;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.util.MachineTagUtils;
import org.gbif.cli.ConfigUtils;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.ws.client.guice.CrawlerWsClientModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.ReadableInstant;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.vocabulary.TagName.DECLARED_RECORD_COUNT;

/**
 * This service scans the registry at a configurable interval and schedules a crawl for Datasets that fulfill a few
 * conditions, up to a configured maximum number of datasets per run:
 * <ul>
 * <li>The dataset is not deleted</li>
 * <li>The dataset is not external</li>
 * <li>The dataset has never been crawled before</li>
 * <li>The dataset hasn't been crawled within the permissible time window</li>
 * <li>The dataset is not tagged in a way that prevent auto-scheduled crawling</li>
 * </ul>
 */
public class CrawlSchedulerService extends AbstractScheduledService {
  public static final String CRAWLER_NAMESPACE = "crawler.gbif.org";
  public static final String TAG_EXCLUDE_FROM_SCHEDULED_CRAWL = "noScheduledCrawl";
  private static final Logger LOG = LoggerFactory.getLogger(CrawlSchedulerService.class);
  private final CrawlSchedulerConfiguration configuration;
  private DefaultMessagePublisher publisher;
  private DatasetService datasetService;
  private DatasetProcessService crawlService;
  private DatasetProcessStatusService registryService;

  public CrawlSchedulerService(CrawlSchedulerConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void runOneIteration() throws Exception {
    LOG.info("Checking for datasets to crawl now");
    ReadableInstant now = new DateTime();

    PagingRequest pageable = new PagingRequest();
    PagingResponse<Dataset> datasets;
    int numberInitiated = 0;
    do {
      if (numberInitiated >= configuration.maximumCrawlsPerRun) {
        LOG.info("Reached limit of how many crawls to initiate per iteration [{}]", numberInitiated);
        break;
      }
      datasets = datasetService.list(pageable);
      for (Dataset dataset : datasets.getResults()) {
        if (isDatasetEligibleToCrawl(dataset, now)) {
          LOG.debug("Eligible to crawl [{}]", dataset.getKey());
          startCrawl(dataset);
          numberInitiated++;
        } else {
          LOG.debug("Not eligible to crawl [{}]", dataset.getKey());
        }
      }
      pageable.nextPage();
    } while (!datasets.isEndOfRecords());
    LOG.info("Finished checking for datasets, having initiated {} crawls", numberInitiated);
  }

  @Override
  protected void startUp() throws Exception {
    publisher = new DefaultMessagePublisher(configuration.messaging.getConnectionParameters());

    Properties props = ConfigUtils.toProperties(configuration);

    Injector injector = Guice
      .createInjector(new CrawlerWsClientModule(props), new RegistryWsClientModule(props), new AnonymousAuthModule());
    datasetService = injector.getInstance(DatasetService.class);
    crawlService = injector.getInstance(DatasetProcessService.class);
    registryService = injector.getInstance(DatasetProcessStatusService.class);
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, configuration.interval, TimeUnit.MINUTES);
  }

  /**
   * This method checks whether a dataset should automatically be scheduled to crawl or not.
   *
   * @return {@code true} if the dataset should be crawled
   */
  private boolean isDatasetEligibleToCrawl(Dataset dataset, ReadableInstant now) {
    if (dataset.getDeleted() != null) {
      return false;
    }

    if (dataset.isExternal()) {
      return false;
    }

    PagingResponse<DatasetProcessStatus> list = registryService.listDatasetProcessStatus(dataset.getKey(), null);

    // We always crawl datasets that haven't been crawled before
    if (list.getResults().isEmpty() ) {
      return true;
    }

    DatasetProcessStatus status = list.getResults().get(0);
    ReadableInstant lastCrawlDate = new DateTime(status.getFinishedCrawling());

    // Check whether if it was last crawled in the permissible window
    if (Days.daysBetween(lastCrawlDate, now).getDays() < configuration.maxLastCrawledInDays) {
      return false;
    }

    // Check whether there is a machine tag that omits this dataset from crawling
    if (tagsExcludeFromScheduledCrawling(dataset)) {
      return false;
    }

    // Check whether the Dataset is currently being crawled
    return crawlService.getDatasetProcessStatus(dataset.getKey()) == null;
  }

  private void startCrawl(Dataset dataset) {
    try {
      Message message = new StartCrawlMessage(dataset.getKey(), StartCrawlMessage.Priority.LOW);
      publisher.send(message);
      LOG.info("Sent message to crawl [{}]", dataset.getKey());
    } catch (IOException e) {
      LOG.error("Caught exception while sending crawl message", e);
    }
  }

  /**
   * Returns true if there is a machine tag on the registry that excludes the dataset from being crawled on a schedule.
   */
  private boolean tagsExcludeFromScheduledCrawling(Dataset dataset) {
    List<MachineTag> filteredTags = MachineTagUtils.list(dataset, CRAWLER_NAMESPACE, TAG_EXCLUDE_FROM_SCHEDULED_CRAWL);
    for (MachineTag tag : filteredTags) {
      if (Boolean.parseBoolean(tag.getValue())) {
        LOG.info("Dataset [{}] has been tagged [{}:{}] to be excluded from scheduled crawling", dataset.getKey(),
                 CRAWLER_NAMESPACE, TAG_EXCLUDE_FROM_SCHEDULED_CRAWL);
        return true;
      }
    }
    return false;
  }
}
