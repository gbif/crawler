package org.gbif.crawler;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.api.service.crawler.DatasetProcessService;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.queue.QueueHelper;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.retry.RetryNTimes;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_CONTEXT;
import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_INFO;
import static org.gbif.crawler.constants.CrawlerNodePaths.DECLARED_COUNT;
import static org.gbif.crawler.constants.CrawlerNodePaths.DWCA_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_EMITTED;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_PROCESSED;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_RECEIVED;
import static org.gbif.crawler.constants.CrawlerNodePaths.INTERPRETED_OCCURRENCES_PERSISTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.INTERPRETED_OCCURRENCES_PERSISTED_SUCCESSFUL;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_CRAWLED;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_SUCCESSFUL;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.QUEUED_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_NEW;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_UNCHANGED;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_UPDATED;
import static org.gbif.crawler.constants.CrawlerNodePaths.RUNNING_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.STARTED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.VERBATIM_OCCURRENCES_PERSISTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.VERBATIM_OCCURRENCES_PERSISTED_SUCCESSFUL;
import static org.gbif.crawler.constants.CrawlerNodePaths.XML_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.buildPath;
import static org.gbif.crawler.constants.CrawlerNodePaths.getCrawlInfoPath;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This {@link DatasetProcessService} implementation uses a {@link CuratorFramework} instance to communicate with
 * a Zookeeper server.
 */
public class DatasetProcessServiceImpl implements DatasetProcessService {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetProcessServiceImpl.class);
  private static final QueueSerializer<UUID> UUID_SERIALIZER = new UuidSerializer();
  // 10 attempts @ 100 msec interval, we can make this configurable if needed
  private final RetryPolicy counterRetryPolicy = new RetryNTimes(10, 100);
  private final CuratorFramework curator;
  private final ObjectMapper mapper;
  private final Executor executor;

  /**
   * Creates a CrawlerMetricsService. Responsible for interacting with a ZooKeeper instance in a read-only fashion.
   *
   * @param curator to access ZooKeeper
   * @param mapper  to deserialize CrawlJob JSON back into an object
   * @param executor to run the thread pool
   */
  @Inject
  public DatasetProcessServiceImpl(CuratorFramework curator, ObjectMapper mapper, Executor executor) {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.mapper = checkNotNull(mapper, "mapper can't be null");
    this.executor = checkNotNull(executor, "executor can't be null");
  }

  // TODO: This message need to be much more lenient with bad data
  @Override
  public DatasetProcessStatus getDatasetProcessStatus(UUID datasetKey) {
    checkNotNull(datasetKey, "datasetKey can't be null");

    // Check if dataset is actually being processed right now
    try {
      if (curator.checkExists().forPath(getCrawlInfoPath(datasetKey)) == null) {
        return null;
      }
    } catch (Exception e) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", e);
    }

    DatasetProcessStatus.Builder builder = DatasetProcessStatus.builder();

    String crawlPath = getCrawlInfoPath(datasetKey);
    builder.datasetKey(datasetKey);

    // Here we're trying to load all information from Zookeeper into the DatasetProcessStatus object
    try {
      byte[] crawlJobBytes = curator.getData().forPath(crawlPath);
      CrawlJob crawlJob = mapper.readValue(crawlJobBytes, CrawlJob.class);
      builder.crawlJob(crawlJob);

      String path = getCrawlInfoPath(datasetKey, CRAWL_CONTEXT);
      if (curator.checkExists().forPath(path) != null) {
        byte[] responseData = curator.getData().forPath(path);
        if (responseData != null) {
          builder.crawlContext(new String(responseData, Charsets.UTF_8));
        }
      }

      path = getCrawlInfoPath(datasetKey, DECLARED_COUNT);
      if (curator.checkExists().forPath(path) != null) {
        byte[] responseData = curator.getData().forPath(path);
        builder.declaredCount(Long.valueOf(new String(responseData)));
      }

      // should there be no started crawling, nothing else is read for now, later we might have to remove this
      // optimization as we could pause and resume crawls
      path = getCrawlInfoPath(datasetKey, STARTED_CRAWLING);
      if (curator.checkExists().forPath(path) != null) {
        byte[] responseData = curator.getData().forPath(path);
        builder.startedCrawling(asDate(responseData));

        builder.processStateOccurrence(getState(datasetKey, PROCESS_STATE_OCCURRENCE));
        builder.processStateChecklist(getState(datasetKey, PROCESS_STATE_CHECKLIST));
        builder.processStateSample(getState(datasetKey, PROCESS_STATE_SAMPLE));

        path = getCrawlInfoPath(datasetKey, FINISHED_CRAWLING);
        if (curator.checkExists().forPath(path) != null) {
          responseData = curator.getData().forPath(path);
          builder.finishedCrawling(asDate(responseData));

          responseData = curator.getData().forPath(getCrawlInfoPath(datasetKey, FINISHED_REASON));
          builder.finishReason(FinishReason.valueOf(new String(responseData)));
        }

        builder.pagesCrawled(getCounter(crawlPath, PAGES_CRAWLED).or(0L));
        builder.pagesFragmentedSuccessful(getCounter(crawlPath, PAGES_FRAGMENTED_SUCCESSFUL).or(0L));
        builder.pagesFragmentedError(getCounter(crawlPath, PAGES_FRAGMENTED_ERROR).or(0L));
        builder.fragmentsEmitted(getCounter(crawlPath, FRAGMENTS_EMITTED).or(0L));
        builder.fragmentsReceived(getCounter(crawlPath, FRAGMENTS_RECEIVED).or(0L));
        builder.rawOccurrencesPersistedNew(getCounter(crawlPath, RAW_OCCURRENCES_PERSISTED_NEW).or(0L));
        builder.rawOccurrencesPersistedUpdated(getCounter(crawlPath, RAW_OCCURRENCES_PERSISTED_UPDATED).or(0L));
        builder.rawOccurrencesPersistedUnchanged(getCounter(crawlPath, RAW_OCCURRENCES_PERSISTED_UNCHANGED).or(0L));
        builder.rawOccurrencesPersistedError(getCounter(crawlPath, RAW_OCCURRENCES_PERSISTED_ERROR).or(0L));
        builder.fragmentsProcessed(getCounter(crawlPath, FRAGMENTS_PROCESSED).or(0L));
        builder.verbatimOccurrencesPersistedSuccessful(getCounter(crawlPath,
                                                                  VERBATIM_OCCURRENCES_PERSISTED_SUCCESSFUL).or(0L));
        builder.verbatimOccurrencesPersistedError(getCounter(crawlPath, VERBATIM_OCCURRENCES_PERSISTED_ERROR).or(0L));
        builder.interpretedOccurrencesPersistedSuccessful(getCounter(crawlPath,
                                                                     INTERPRETED_OCCURRENCES_PERSISTED_SUCCESSFUL).or(0L));
        builder.interpretedOccurrencesPersistedError(getCounter(crawlPath, INTERPRETED_OCCURRENCES_PERSISTED_ERROR).or(
          0L));
      }

    } catch (Exception e) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper, getting status for "
                                            + datasetKey.toString(), e);
    }
    return builder.build();
  }

  @Override
  public Set<DatasetProcessStatus> getRunningDatasetProcesses() {
    List<UUID> pendingUuids = getPendingCrawlUuids(XML_CRAWL);
    pendingUuids.addAll(getPendingCrawlUuids(DWCA_CRAWL));
    List<String> allCrawls = getChildren(CRAWL_INFO, false);
    List<UUID> allCrawlUuids = Lists.newArrayList();
    for (String crawl : allCrawls) {
      allCrawlUuids.add(UUID.fromString(crawl));
    }

    allCrawlUuids.removeAll(pendingUuids);

    return Sets.newHashSet(getDatasetProcessStatuses(allCrawlUuids));
  }

  @Override
  public List<DatasetProcessStatus> getPendingXmlDatasetProcesses() {
    List<UUID> pendingCrawlUuids = getPendingCrawlUuids(XML_CRAWL);
    return getDatasetProcessStatuses(pendingCrawlUuids);
  }

  @Override
  public List<DatasetProcessStatus> getPendingDwcaDatasetProcesses() {
    List<UUID> pendingCrawlUuids = getPendingCrawlUuids(DWCA_CRAWL);
    return getDatasetProcessStatuses(pendingCrawlUuids);
  }

  /**
   * Gets all pending but not running crawls from the specified path (either XML or DwC-A queue usually)
   *
   * @param path which needs two sub nodes: one for queued and one for running crawls
   */
  private List<UUID> getPendingCrawlUuids(String path) {
    checkNotNull(path, "path can't be null");
    LOG.debug("Requested a list of all queued datasets");

    // Gets the list of running and queued queue IDs
    List<String> runningIdentifiers = getChildren(buildPath(path, RUNNING_CRAWLS), false);
    List<String> queueIdentifiers = getChildren(buildPath(path, QUEUED_CRAWLS), true);

    // retain identifiers that are only present in the queued-jobs queue, so we are left with all queue identifiers
    // that are _not_ currently being worked on
    queueIdentifiers.removeAll(runningIdentifiers);

    LOG.debug("[{}] queued datasets, retrieving UUIDs", queueIdentifiers.size());
    List<UUID> queueKeys = getQueueKeys(buildPath(path, QUEUED_CRAWLS), queueIdentifiers);
    LOG.debug("Retrieved UUIDs for queued datasets");

    return queueKeys;
  }

  /**
   * This method will retrieve the processing status for a list of UUIDs
   *
   * @param queueKeys uuids to query
   *
   * @return list of processing statuses, at the moment no guarantees are made for ordering
   */
  private List<DatasetProcessStatus> getDatasetProcessStatuses(Collection<UUID> queueKeys) {
    CompletionService<DatasetProcessStatus> completionService =
      new ExecutorCompletionService<DatasetProcessStatus>(executor);
    for (final UUID queueKey : queueKeys) {
      completionService.submit(new Callable<DatasetProcessStatus>() {
        @Override
        public DatasetProcessStatus call() throws Exception {
          return getDatasetProcessStatus(queueKey);
        }
      });
    }

    List<DatasetProcessStatus> processStatuses = Lists.newArrayList();

    for (int i = 0; i < queueKeys.size(); i++) {
      try {
        Future<DatasetProcessStatus> future = completionService.take();
        DatasetProcessStatus status = future.get();
        processStatuses.add(status);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.warn("Caught exception trying to retrieve dataset", e.getCause());
        // TODO: Commented out because we don't fail if ZK is inconsistent for a second. On the other hand if we
        // get only exceptions we want to capture that somehow and throw something in the end.
        //throw new ServiceUnavailableException("Exception while getting dataset process status", e.getCause());
      }
    }
    return processStatuses;
  }

  /**
   * This gets a counter which has been written using a {@link DistributedAtomicLong} if it exists.
   *
   * @param rootPath where this counter is supposed to be in
   * @param counter  to read
   *
   * @return the counter value or absent if it didn't exist
   */
  private Optional<Long> getCounter(String rootPath, String counter) {
    DistributedAtomicLong dal = new DistributedAtomicLong(curator, rootPath + "/" + counter, counterRetryPolicy);
    try {
      return Optional.fromNullable(dal.get().preValue());
    } catch (Exception ignored) {
      return Optional.absent();
    }
  }

  private ProcessState getState(UUID datasetKey, String statePath) {
    try {
      String path = getCrawlInfoPath(datasetKey, statePath);
      if (curator.checkExists().forPath(path) != null) {
        byte[] responseData = curator.getData().forPath(path);
        return ProcessState.valueOf(new String(responseData));
      }
    } catch (Exception e) {
    }
    return null;
  }

    /**
   * Gets a list of {@link UUID} keys belonging to datasets. Each dataset key is associated to one
   * queue identifier.
   *
   * @param path             of the queue (the actual path to the queue not a parent node and not the running crawls)
   * @param queueIdentifiers queue identifiers that might or might not contain a dataset {@link UUID} key
   *
   * @return list of dataset {@link UUID} keys
   */
  private List<UUID> getQueueKeys(final String path, Iterable<String> queueIdentifiers) {
    CompletionService<UUID> completionService = new ExecutorCompletionService<UUID>(executor);
    List<Future<UUID>> futures = Lists.newArrayList();
    for (final String queueIdentifier : queueIdentifiers) {
      Future<UUID> future = completionService.submit(new Callable<UUID>() {
        @Override
        public UUID call() throws Exception {
          byte[] responseData = curator.getData().forPath(buildPath(path, queueIdentifier));
          return QueueHelper.deserializeSingle(responseData, UUID_SERIALIZER);
        }
      });
      futures.add(future);
    }

    List<UUID> keys = Lists.newArrayList();
    for (Future<UUID> future : futures) {
      try {
        keys.add(future.get());
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        throw new ServiceUnavailableException("Exception while getting dataset process status", e.getCause());
      }
    }
    LOG.debug("Retrieved all queued dataset process statuses");
    return keys;
  }

  /**
   * Given a path, returns all the child nodes, optionally lexicographically sorted.
   *
   * @param path   to get a list of queue identifiers
   * @param sorted whether to sort the child nodes lexicographically
   *
   * @return a list of child nodes
   */
  private List<String> getChildren(String path, boolean sorted) {
    List<String> identifiers;
    try {
      identifiers = curator.getChildren().forPath(path);
    } catch (Exception e) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", e);
    }

    if (sorted) {
      Collections.sort(identifiers, String.CASE_INSENSITIVE_ORDER);
    }
    return identifiers;
  }

  /**
   * Utility to convert a date as an array of bytes to proper {@link Date} format.
   *
   * @param dateAsBytes a date as a byte array
   *
   * @return The properly formatted date
   */
  private Date asDate(byte[] dateAsBytes) {
    if (dateAsBytes == null) {
      return null;
    }

    String dateAsString = new String(dateAsBytes, Charsets.UTF_8);
    try {
      // TODO: is the timezone right? Check with real data coming from zookeeper.
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
      return sdf.parse(dateAsString);
    } catch (ParseException pe) {
      LOG.warn("Date was not parsed successfully: [{}]: ", dateAsString, pe);
    }
    return null;
  }

}
