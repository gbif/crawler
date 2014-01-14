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

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.cli.ConfigUtils;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.crawler.ws.client.guice.CrawlerWsClientModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service scans the registry at a configurable interval and schedules a crawl for all Datasets that fulfill a few
 * conditions:
 * <ul>
 * <li>The dataset is not deleted</li>
 * <li>The dataset is not external</li>
 * <li>The dataset has never been crawled before</li>
 * <li>The dataset hasn't been crawled in a week</li>
 * </ul>
 */
public class CrawlSchedulerService extends AbstractScheduledService {

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
    do {
      datasets = datasetService.list(pageable);
      for (Dataset dataset : datasets.getResults()) {
        if (isDatasetEligibleToCrawl(dataset, now)) {
          LOG.debug("Eligible to crawl [{}]", dataset.getKey());
          startCrawl(dataset);
        } else {
          LOG.debug("Not eligible to crawl [{}]", dataset.getKey());
        }
      }
      pageable.nextPage();
    } while (!datasets.isEndOfRecords());
    LOG.info("Finished checking for datasets");
  }

  @Override
  protected void startUp() throws Exception {
    publisher = new DefaultMessagePublisher(configuration.messaging.getConnectionParameters());

    Properties props = ConfigUtils.toProperties(configuration);

    Injector injector = Guice.createInjector(new CrawlerWsClientModule(props),
                                             new RegistryWsClientModule(props),
                                             new AnonymousAuthModule());
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
    if (list.getResults().isEmpty()) {
      return true;
    }

    DatasetProcessStatus status = list.getResults().get(0);
    ReadableInstant lastCrawlDate = new DateTime(status.getFinishedCrawling());

    // Check whether less than a week has passed since the last crawl
    if (Weeks.weeksBetween(lastCrawlDate, now).getWeeks() < 1) {
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

}
