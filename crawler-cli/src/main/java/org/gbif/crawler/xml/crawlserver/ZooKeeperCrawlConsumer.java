package org.gbif.crawler.xml.crawlserver;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.Crawler;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;
import org.gbif.crawler.xml.crawlserver.builder.CrawlerBuilder;
import org.gbif.crawler.xml.crawlserver.listener.CrawlerZooKeeperUpdatingListener;
import org.gbif.crawler.xml.crawlserver.listener.LoggingCrawlListener;
import org.gbif.crawler.xml.crawlserver.listener.MessagingCrawlListener;
import org.gbif.crawler.xml.crawlserver.listener.ResultPersistingListener;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.http.HttpResponse;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;

class ZooKeeperCrawlConsumer implements QueueConsumer<UUID> {

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCrawlConsumer.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new GuavaModule());
  }

  private static final int DEFAULT_CONCURRENT_CONNECTIONS = 2;
  private static final String CONCURRENT_CONNECTIONS_PROPERTY = "concurrentConnections";

  private final CuratorFramework curator;
  private final MessagePublisher publisher;
  private final Optional<Long> minDelay;
  private final Optional<Long> maxDelay;
  private final File responseArchive;

  /**
   * @param minDelay is the "grace" period in milliseconds we allow between calls to a URL implemented by waiting to
   *        release the lock on the URL for this amount of time
   */
  ZooKeeperCrawlConsumer(
    CuratorFramework curator,
    MessagePublisher publisher,
    Optional<Long> minDelay,
    Optional<Long> maxDelay,
    File responseArchive) {
    this.curator = curator;
    this.publisher = publisher;
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
    this.responseArchive = responseArchive;
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    LOG.info("State changed to [{}]", newState);
  }

  @Override
  public void consumeMessage(UUID message) throws Exception {
    LOG.debug("Got crawl job for UUID [{}]", message);
    MDC.put("datasetKey", message.toString());

    try {
      byte[] bytes = curator.getData().forPath(CrawlerNodePaths.getCrawlInfoPath(message));
      CrawlJob crawlJob = MAPPER.readValue(bytes, CrawlJob.class);
      LOG.debug("Crawl job detail [{}]", crawlJob);

      CrawlerBuilder builder =
        CrawlerBuilder.buildFor(crawlJob).withDefaultRetryStrategy().withScientificNameRangeCrawlContext();

      int concurrentConnections = DEFAULT_CONCURRENT_CONNECTIONS;
      if (crawlJob.getProperties().containsKey(CONCURRENT_CONNECTIONS_PROPERTY)) {
        concurrentConnections = Integer.parseInt(crawlJob.getProperty(CONCURRENT_CONNECTIONS_PROPERTY));
      }

      builder.withZooKeeperLock(curator, concurrentConnections);

      if (minDelay.isPresent()) {
        builder.withDelayedLock(minDelay.get(), maxDelay.get());
      }

      Crawler<ScientificNameRangeCrawlContext, String, HttpResponse, List<Byte>> crawler = builder.build();

      crawler.addListener(new CrawlerZooKeeperUpdatingListener<ScientificNameRangeCrawlContext>(builder
        .getCrawlConfiguration(),
        curator));
      crawler.addListener(new LoggingCrawlListener<ScientificNameRangeCrawlContext>(builder.getCrawlConfiguration()));
      crawler.addListener(new MessagingCrawlListener<ScientificNameRangeCrawlContext>(publisher,
        builder.getCrawlConfiguration()));
      if (responseArchive != null) {
        crawler.addListener(new ResultPersistingListener(responseArchive, builder.getCrawlConfiguration()));
      }

      // This blocks until the crawl is finished.
      crawler.crawl();
    } catch (Exception e) {
      // If we catch an exception here that wasn't handled before then something bad happened and we'll abort
      // TODO: This is a dirty copy & paste job and should be replaced with a library but we don't have access to the
      // CrawlerZooKeeperUpdatingListener at this point.
      LOG.warn("Caught exception while crawling. Aborting crawl.", e);

      String crawlPath = CrawlerNodePaths.getCrawlInfoPath(message, FINISHED_CRAWLING);
      Date date = new Date();
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
      byte[] data = dateFormat.format(date).getBytes(Charsets.UTF_8);
      createOrUpdateZooKeeper(crawlPath, data);
      crawlPath = CrawlerNodePaths.getCrawlInfoPath(message, FINISHED_REASON);
      createOrUpdateZooKeeper(crawlPath, FinishReason.ABORT.toString().getBytes(Charsets.UTF_8));
    } finally {
      LOG.debug("Finished crawling [{}]", message);
      MDC.remove("datasetKey");
    }
  }

  private void createOrUpdateZooKeeper(String crawlPath, byte[] data) {
    try {
      Stat stat = curator.checkExists().forPath(crawlPath);
      if (stat == null) {
        curator.create().forPath(crawlPath, data);
      } else {
        curator.setData().forPath(crawlPath, data);
      }
    } catch (Exception e1) {
      LOG.error("Exception while updating ZooKeeper", e1);
    }
  }

}
