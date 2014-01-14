package org.gbif.crawler;


import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_CONTEXT;
import static org.gbif.crawler.constants.CrawlerNodePaths.DECLARED_COUNT;
import static org.gbif.crawler.constants.CrawlerNodePaths.DWCA_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_EMITTED;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_PROCESSED;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_RECEIVED;
import static org.gbif.crawler.constants.CrawlerNodePaths.INTERPRETED_OCCURRENCES_PERSISTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.INTERPRETED_OCCURRENCES_PERSISTED_SUCCESSFUL;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_CRAWLED;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_SUCCESSFUL;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_NEW;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_UNCHANGED;
import static org.gbif.crawler.constants.CrawlerNodePaths.RAW_OCCURRENCES_PERSISTED_UPDATED;
import static org.gbif.crawler.constants.CrawlerNodePaths.STARTED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.VERBATIM_OCCURRENCES_PERSISTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.VERBATIM_OCCURRENCES_PERSISTED_SUCCESSFUL;
import static org.gbif.crawler.constants.CrawlerNodePaths.XML_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.buildPath;

import static org.apache.curator.framework.recipes.queue.QueueHelper.serialize;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DatasetProcessServiceImplTest {

  private static TestingServer server;
  private static CuratorFramework curator;
  private static final Map<DatasetProcessStatus, String> QUEUE_MAP = Maps.newHashMap();
  private static final Set<DatasetProcessStatus> ALL_CRAWLS = Sets.newHashSet();
  private static final Set<DatasetProcessStatus> RUNNING_CRAWLS = Sets.newHashSet();
  private static List<DatasetProcessStatus> QUEUED_CRAWLS = Lists.newArrayList();

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final String CRAWL_NAMESPACE = "crawler";
  private static final QueueSerializer<UUID> UUID_SERIALIZER = new UuidSerializer();

  private final DatasetProcessServiceImpl service;

  public DatasetProcessServiceImplTest() {
    service = new DatasetProcessServiceImpl(curator, new ObjectMapper(), Executors.newSingleThreadExecutor());
  }

  @BeforeClass
  public static void setup() throws Exception {
    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder()
      .connectString(server.getConnectString())
      .namespace(CRAWL_NAMESPACE)
      .retryPolicy(new RetryOneTime(1))
      .build();
    curator.start();

    curator.create()
      .creatingParentsIfNeeded()
      .forPath(buildPath(DWCA_CRAWL, CrawlerNodePaths.RUNNING_CRAWLS));

    curator.create()
      .creatingParentsIfNeeded()
      .forPath(buildPath(DWCA_CRAWL, CrawlerNodePaths.QUEUED_CRAWLS));

    for (int i = 0; i < 10; i++) {
      UUID uuid = UUID.randomUUID();
      setupTestCrawl(uuid);
    }

  }

  @AfterClass
  public static void tearDown() throws IOException {
    curator.close();
    server.stop();
  }

  /**
   * Sets up a test crawl by generating an entry in the queue list. With a 30% chance an added crawl will also be added
   * to the running list.
   *
   * @param uuid to add
   */
  private static void setupTestCrawl(UUID uuid) throws Exception {
    Random random = new Random();

    // Simulates a curator queue identifier by generating a random number and then padding with '0' to 19 chars
    String queueIdentifier = "queue-" + Strings.padStart(String.valueOf(Math.abs(random.nextLong())), 19, '0');
    curator.create()
      .creatingParentsIfNeeded()
      .forPath(buildPath(XML_CRAWL, CrawlerNodePaths.QUEUED_CRAWLS, queueIdentifier), serialize(uuid, UUID_SERIALIZER));

    // 30% chance of this crawl being a running one
    boolean running = false;
    if (random.nextInt(100) < 30) {
      running = true;
      curator.create()
        .creatingParentsIfNeeded()
        .forPath(buildPath(XML_CRAWL, CrawlerNodePaths.RUNNING_CRAWLS, queueIdentifier),
                 serialize(uuid, UUID_SERIALIZER));
    }

    // get a DatasetCrawlMetrics mock object to populate the first crawl job
    DatasetProcessStatus status = mockMetrics(uuid, running);
    // populate the first crawl job with complete data
    String runningJob = CrawlerNodePaths.getCrawlInfoPath(uuid);

    curator.create()
      .creatingParentsIfNeeded()
      .forPath(buildPath(runningJob, STARTED_CRAWLING), dateToBytes(status.getStartedCrawling()));
    curator.create()
      .creatingParentsIfNeeded()
      .forPath(buildPath(runningJob, CRAWL_CONTEXT), stringToBytes(status.getCrawlContext()));

    curator.setData().forPath(runningJob, new ObjectMapper().writeValueAsBytes(status.getCrawlJob()));

    if (status.getDeclaredCount() != null) {
      curator.create()
        .forPath(buildPath(runningJob, DECLARED_COUNT), status.getDeclaredCount().toString().getBytes(Charsets.UTF_8));
    }

    setCounter(buildPath(runningJob, PAGES_CRAWLED), status.getPagesCrawled());
    setCounter(buildPath(runningJob, PAGES_FRAGMENTED_SUCCESSFUL), status.getPagesFragmentedSuccessful());
    setCounter(buildPath(runningJob, PAGES_FRAGMENTED_ERROR), status.getPagesFragmentedError());
    setCounter(buildPath(runningJob, FRAGMENTS_EMITTED), status.getFragmentsEmitted());
    setCounter(buildPath(runningJob, FRAGMENTS_RECEIVED), status.getFragmentsReceived());
    setCounter(buildPath(runningJob, RAW_OCCURRENCES_PERSISTED_NEW), status.getRawOccurrencesPersistedNew());
    setCounter(buildPath(runningJob, RAW_OCCURRENCES_PERSISTED_UPDATED), status.getRawOccurrencesPersistedUpdated());
    setCounter(buildPath(runningJob, RAW_OCCURRENCES_PERSISTED_UNCHANGED),
               status.getRawOccurrencesPersistedUnchanged());
    setCounter(buildPath(runningJob, RAW_OCCURRENCES_PERSISTED_ERROR), status.getRawOccurrencesPersistedError());
    setCounter(buildPath(runningJob, FRAGMENTS_PROCESSED), status.getFragmentsProcessed());
    setCounter(buildPath(runningJob, VERBATIM_OCCURRENCES_PERSISTED_SUCCESSFUL),
               status.getVerbatimOccurrencesPersistedSuccessful());
    setCounter(buildPath(runningJob, VERBATIM_OCCURRENCES_PERSISTED_ERROR),
               status.getVerbatimOccurrencesPersistedError());
    setCounter(buildPath(runningJob, INTERPRETED_OCCURRENCES_PERSISTED_SUCCESSFUL),
               status.getInterpretedOccurrencesPersistedSuccessful());
    setCounter(buildPath(runningJob, INTERPRETED_OCCURRENCES_PERSISTED_ERROR),
               status.getInterpretedOccurrencesPersistedError());

    ALL_CRAWLS.add(status);
    if (running) {
      RUNNING_CRAWLS.add(status);
    } else {
      QUEUED_CRAWLS.add(status);
      QUEUE_MAP.put(status, queueIdentifier);
    }

    Ordering<DatasetProcessStatus> ordering =
      Ordering.natural().onResultOf(new Function<DatasetProcessStatus, String>() {
        @Override
        public String apply(@Nullable DatasetProcessStatus input) {
          return QUEUE_MAP.get(input);
        }
      });
    QUEUED_CRAWLS = ordering.sortedCopy(QUEUED_CRAWLS);

  }

  private static void setCounter(String path, long value) throws Exception {
    curator.create().creatingParentsIfNeeded().forPath(path);
    DistributedAtomicLong dal = new DistributedAtomicLong(curator, path, new RetryOneTime(100));
    dal.trySet(value);
  }

  /**
   * Builds a mock @{link DatasetCrawlMetrics} object which represents the possible metrics values.
   *
   * @param datasetKey the key associated to the dataset being crawled
   *
   * @return the mocked object
   */
  private static DatasetProcessStatus mockMetrics(UUID datasetKey, boolean running) {

    if (running) {
      Date date = new Date();
      date = new Date((date.getTime() / 1000) * 1000);
      String crawlContext = "{\"offset\":0,\"aborted\":false,\"lowerBound\":\"zza\",\"upperBound\":null}";
      return DatasetProcessStatus.builder()
        .crawlJob(new CrawlJob(datasetKey, EndpointType.BIOCASE, URI.create("http://www.running.com"), 1, null))
        .crawlContext(crawlContext)
        .datasetKey(datasetKey)
        .startedCrawling(date)
        .declaredCount(100L)
        .pagesCrawled(10)
        .pagesFragmentedSuccessful(7L)
        .pagesFragmentedError(2L)
        .fragmentsEmitted(80L)
        .fragmentsReceived(70L)
        .rawOccurrencesPersistedNew(50L)
        .rawOccurrencesPersistedUnchanged(10L)
        .rawOccurrencesPersistedUpdated(5L)
        .rawOccurrencesPersistedError(1L)
        .fragmentsProcessed(66L)
        .verbatimOccurrencesPersistedSuccessful(50L)
        .verbatimOccurrencesPersistedError(5L)
        .interpretedOccurrencesPersistedSuccessful(40L)
        .interpretedOccurrencesPersistedError(5L)
        .build();
    } else {
      return DatasetProcessStatus.builder()
        .crawlJob(new CrawlJob(datasetKey, EndpointType.BIOCASE, URI.create("http://www.queued.com"), 1, null))
        .datasetKey(datasetKey)
        .declaredCount(100L)
        .build();
    }
  }

  /**
   * Tests the current running crawls.
   */
  @Test
  public void testGetRunningDatasetCrawls() {
    Set<DatasetProcessStatus> crawls = service.getRunningDatasetProcesses();
    assertEquals(RUNNING_CRAWLS, crawls);
  }

  /**
   * Tests the currently queued crawls.
   */
  // TODO: Should test for same order
  @Test
  public void testGetQueuedDatasetCrawls() {
    List<DatasetProcessStatus> crawls = service.getPendingXmlDatasetProcesses();
    for (DatasetProcessStatus datasetProcessStatus : QUEUED_CRAWLS) {
      assertThat(crawls, hasItem(datasetProcessStatus));
    }
  }

  /**
   * Tests whether a crawl job has all the possible fields populated.
   */
  @Test
  public void testFullGetCrawlMetrics() {
    for (DatasetProcessStatus mockStatus : ALL_CRAWLS) {
      DatasetProcessStatus realStatus = service.getDatasetProcessStatus(mockStatus.getDatasetKey());
      assertEquals(mockStatus, realStatus);
    }
  }

  /**
   * Simple conversion from a {@link Date} into an array of bytes.
   *
   * @param date the date to convert
   *
   * @return the array of bytes representing the date
   */
  private static byte[] dateToBytes(Date date) {
    if (date != null) {
      SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
      String dateAsString = sdf.format(date);
      return dateAsString.getBytes();
    }
    return null;
  }


  /**
   * Simple conversion from a {@link String} into an array of bytes.
   *
   * @param value the string to convert
   *
   * @return the array of bytes representing the string
   */
  private static byte[] stringToBytes(String value) {
    if (value != null) {
      return value.getBytes();
    }
    return null;
  }
}
