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
package org.gbif.crawler.scheduler;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.util.MachineTagUtils;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.crawler.ws.client.DatasetProcessClient;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.DatasetProcessStatusClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 * This service scans the registry at a configurable interval and schedules a crawl for Datasets that fulfill a few
 * conditions:
 * <ul>
 * <li>The dataset is not deleted</li>
 * <li>The dataset is not external</li>
 * <li>The dataset has never been crawled before</li>
 * <li>The dataset hasn't been crawled within the permissible time window</li>
 * <li>The dataset is not tagged in a way that prevent auto-scheduled crawling</li>
 * </ul>
 * <p/>
 * This reads the load on the crawler before initiating any crawls, allowing self throttling behaviour.
 */
@SuppressWarnings("UnstableApiUsage")
public class CrawlSchedulerService extends AbstractScheduledService {
  public static final String CRAWLER_NAMESPACE = "crawler.gbif.org";
  public static final String TAG_EXCLUDE_FROM_SCHEDULED_CRAWL = "omitFromScheduledCrawl";
  private static final Logger LOG = LoggerFactory.getLogger(CrawlSchedulerService.class);
  private static final Random RANDOM = new Random();
  private final CrawlSchedulerConfiguration configuration;
  private DefaultMessagePublisher publisher;
  private DatasetService datasetService;
  private DatasetProcessService crawlService;
  private DatasetProcessStatusService registryService;

  // Current paging offset, so we don't have to start from 0 every time.
  // (It takes about 30 minutes to go through 70,000 datasets.)
  private PagingRequest pageable = new PagingRequest();

  public CrawlSchedulerService(CrawlSchedulerConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void runOneIteration() {
    LOG.info("Starting checks for datasets eligible to be crawled");

    // first determine if the crawler is already heavily loaded
    int crawlerLoad =
      crawlService.getPendingDwcaDatasetProcesses().size() +
      crawlService.getPendingXmlDatasetProcesses().size() +
      crawlService.getRunningDatasetProcesses().size();
    if (crawlerLoad >= configuration.maximumCrawls) {
      LOG.info("Load on crawler [{}] exceeds target load [{}] - not scheduling any crawling in this iteration",
               crawlerLoad, configuration.maximumCrawls);
      return;
    } else {
      LOG.info("Load on crawler [{}] is below target load [{}] leaving capacity for [{}] new crawls",
               crawlerLoad, configuration.maximumCrawls, configuration.maximumCrawls-crawlerLoad);
    }

    int availableCapacity = configuration.maximumCrawls-crawlerLoad;

    // Log information useful to aid quick diagnostics during operation (e.g. why isn't dataset X scheduling?)
    if (LOG.isInfoEnabled()) {
      for (String datasetKey : configuration.omittedKeys.keySet()) {
        MDC.put("datasetKey", datasetKey);
        LOG.info("Omitting [{}] from auto-scheduled crawling because '{}'.", datasetKey, configuration.omittedKeys.get(datasetKey));
        MDC.remove("datasetKey");
      }

      LOG.info("Supported types: {}", Joiner.on("; ").join(configuration.supportedTypes));
    }

    ReadableInstant now = new DateTime();

    boolean isEnd = false;

    long startingOffset = pageable.getOffset();
    LOG.info("Considering datasets starting from offset {}", startingOffset);
    int numberInitiated = 0;
    int numberConsidered = 0;
    // datasets that have never been crawled are attempted on each run, but only if there is spare capacity.
    // we do this because they typically fail immediately, and we don't want to waste slots on those when accessible
    // data can be found.  At the end of a run, any available slots will be used on these datasets.
    // Remember that new datasets are always attempted on first registration and should be crawled then, so this
    // really represents a list of "bad" datasets.
    List<Dataset> neverCrawledDatasets = Lists.newArrayList();
    while (!isEnd) {
      if (numberInitiated >= availableCapacity) {
        LOG.info("Reached limit of how many crawls to initiate per iteration - capacity was calculated at [{}]",
                 availableCapacity);
        // Could remove limit from the page to ensure not leaving any gaps
        // although probably good enough as it is.
        pageable.setOffset(startingOffset+numberConsidered);
        break;
      }

      List<Dataset> datasetList = Collections.emptyList();

      try {
        PagingResponse<Dataset> datasets = datasetService.list(pageable);
        datasetList = datasets.getResults();
        isEnd = datasets.isEndOfRecords();
      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
      }

      for (Dataset dataset : datasetList) {
        try (MDC.MDCCloseable ignored = MDC.putCloseable("datasetKey", dataset.getKey().toString())) {
          numberConsidered++;
          boolean eligibleToCrawl = false;
          try {
            eligibleToCrawl = isDatasetEligibleToCrawl(dataset, now, neverCrawledDatasets);
          } catch (Exception e) {
            LOG.error("Unexpected exception determining crawl eligibility for dataset[{}].  Swallowing and continuing",
                dataset.getKey(), e);
          }

          if (eligibleToCrawl) {
            startCrawl(dataset);
            numberInitiated++;
            if (numberInitiated >= availableCapacity) {
              LOG.info("Reached limit of how many crawls to initiate per iteration - capacity was calculated at [{}]",
                  availableCapacity);
              pageable.setOffset(startingOffset+numberConsidered);
              break;
            }
          }

          if (numberConsidered % 1000 == 0) {
            LOG.info("Considered {} datasets (at offset {}-{}) so far", numberConsidered, startingOffset, startingOffset+numberConsidered);
          }
        }
      }

      pageable.nextPage();
    }

    if (isEnd) {
      LOG.info("Considered all {} datasets.", startingOffset+numberConsidered);
      pageable = new PagingRequest();
    }

    if (!neverCrawledDatasets.isEmpty() && numberInitiated < availableCapacity) {
      int remainingSlots = availableCapacity - numberInitiated;
      LOG.info("There remain {} available slots having crawled all eligible datasets.  Attempting crawls "
        + "which have never been crawled before [total {}]", remainingSlots, neverCrawledDatasets.size());

      // launch them randomly until we are exhausted
      for (int i = 0; i < remainingSlots && i < neverCrawledDatasets.size(); i++) {
        // randomly select a dataset to ensure all are attempted over time
        int random = RANDOM.nextInt(neverCrawledDatasets.size());
        Dataset dataset = neverCrawledDatasets.get(random);
        try (MDC.MDCCloseable ignored = MDC.putCloseable("datasetKey", dataset.getKey().toString())) {
          startCrawl(dataset);
          neverCrawledDatasets.remove(random);
          LOG.info("Crawling dataset [{}] of type [{}] - has never been successfully crawled",
            dataset.getKey(), dataset.getType());
        }
        numberInitiated++;
      }
    }

    LOG.info("Finished checking for datasets, having considered {} datasets (at offset {}-{}) and initiated {} crawls", numberConsidered,
        startingOffset, startingOffset+numberConsidered, numberInitiated);
  }

  @Override
  protected void startUp() throws Exception {
    publisher = new DefaultMessagePublisher(configuration.messaging.getConnectionParameters());
    ClientBuilder clientBuilder = new ClientBuilder().withUrl(configuration.registryWsUrl)
                                      .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport());
    datasetService = clientBuilder.build(DatasetClient.class);
    crawlService = clientBuilder.build(DatasetProcessClient.class);
    registryService = clientBuilder.build(DatasetProcessStatusClient.class);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shut down the iteration");
    super.shutDown();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, configuration.interval, TimeUnit.MINUTES);
  }

  /**
   * This method checks whether a dataset should automatically be scheduled to crawl or not.
   * Datasets that have never been crawled are added to the supplied list.
   * NOTE: This method is structured for simple readability, and not minimum number of lines of code.
   *
   * @return {@code true} if the dataset should be crawled
   */
  private boolean isDatasetEligibleToCrawl(Dataset dataset, ReadableInstant now, List<Dataset> neverCrawled) {
    if (dataset.getDeleted() != null) {
      LOG.debug("Not eligible to crawl [{}] - deleted dataset of type [{}]", dataset.getKey(), dataset.getType());
      return false;
    }

    if (dataset.isExternal()) {
      LOG.debug("Not eligible to crawl [{}] - external dataset of type [{}]", dataset.getKey(), dataset.getType());
      return false;
    }

    if (dataset.getDuplicateOfDatasetKey() != null) {
      LOG.debug("Not eligible to crawl - dataset [{}] is marked as a duplicate", dataset.getKey());
      return false;
    }

    String type = dataset.getType().name();
    if (!configuration.supportedTypes.contains(type)) {
      LOG.debug("Not eligible to crawl [{}] - type [{}] is not supported in configuration {}",
                dataset.getKey(), dataset.getType(), configuration.supportedTypes);
      return false;
    }

    // configuration can be provided that means the dataset has keys that omit it from scheduled crawling
    if (configurationOmitsDataset(dataset)) {
      return false;
    }

    // Datasets that are constituents are ignored (e.g. checklists that span multiple datasets)
    if (dataset.getParentDatasetKey() != null) {
      LOG.debug("Not eligible to crawl [{}] - is a constituent dataset of type [{}]", dataset.getKey(),
                dataset.getType());
      return false;
    }

    try {
      PagingResponse<DatasetProcessStatus> list = registryService.listDatasetProcessStatus(dataset.getKey(),
          new PagingRequest());

      // We accumulate datasets that have NEVER been successfully crawled
      if (list.getResults().isEmpty() ) {
        neverCrawled.add(dataset);
        LOG.debug("Deferring dataset [{}] of type [{}] - never been successfully crawled", dataset.getKey(),
            dataset.getType());
        return false; // if there is capacity it'll be attempted anyway
      }

      // Check whether if it was last crawled in the permissible window.
      // We prefer looking at when the last crawl finished, but resort to started if needed
      DatasetProcessStatus status = list.getResults().get(0); // last crawl
      ReadableInstant lastCrawlDate = (status.getFinishedCrawling() == null) ?
          new DateTime(status.getStartedCrawling()) :
          new DateTime(status.getFinishedCrawling()); // prefer end date if given
      int days = Days.daysBetween(lastCrawlDate, now).getDays();
      if (days < configuration.maxLastCrawledInDays) {
        LOG.debug("Not eligible to crawl [{}] - crawled {} days ago, which is within threshold of {} days",
            dataset.getKey(), days, configuration.maxLastCrawledInDays);
        return false;
      }

      // Check whether the Dataset is currently being crawled
      if (crawlService.getDatasetProcessStatus(dataset.getKey()) != null) {
        LOG.debug("Not eligible to crawl [{}] - already crawling", dataset.getKey());
        return false;
      }

      // Check whether there is a machine tag that omits this dataset from crawling
      if (tagsExcludeFromScheduledCrawling(dataset)) {
        LOG.debug("Not eligible to crawl [{}] - tagged to be omitted from scheduled crawling [{}:{}]", dataset.getKey(),
            CRAWLER_NAMESPACE, TAG_EXCLUDE_FROM_SCHEDULED_CRAWL);
        return false;
      }

      LOG.info("Crawling dataset [{}] of type [{}]", dataset.getKey(), dataset.getType());
      return true;

    } catch (RuntimeException e) {
      LOG.error("Error determining crawl eligibility for dataset [{}] of type [{}]. Ignore dataset", dataset.getKey(), dataset.getType(), e);
      return false;
    }
  }

  /**
   * Returns true if the installation, organization, parentDataset or key for the given dataset is listed as one to
   * omit in the configuration.
   */
  private boolean configurationOmitsDataset(Dataset dataset) {
    UUID[] uuids = new UUID[]{
      dataset.getKey(),
      dataset.getInstallationKey(),
      dataset.getParentDatasetKey(),
      dataset.getPublishingOrganizationKey(),
    };
    for (UUID uuid : uuids) {
      if (uuid != null) {
        if (configuration.omittedKeys.containsKey(uuid.toString())) {
          LOG.debug("Not eligible to crawl [{}] - {}", dataset.getKey(), configuration.omittedKeys.get(uuid.toString()));
          return true;
        }
      }
    }
    return false;
  }

  private void startCrawl(Dataset dataset) {
    try {
      Message message = new StartCrawlMessage(dataset.getKey(), StartCrawlMessage.Priority.LOW);
      publisher.send(message);
    }  catch (Exception e) {
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
        return true;
      }
    }
    return false;
  }
}
