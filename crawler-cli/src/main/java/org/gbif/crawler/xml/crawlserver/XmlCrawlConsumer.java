package org.gbif.crawler.xml.crawlserver;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.Crawler;
import org.gbif.crawler.common.CrawlConsumer;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;
import org.gbif.crawler.xml.crawlserver.builder.CrawlerBuilder;
import org.gbif.crawler.xml.crawlserver.listener.CrawlerZooKeeperUpdatingListener;
import org.gbif.crawler.xml.crawlserver.listener.LoggingCrawlListener;
import org.gbif.crawler.xml.crawlserver.listener.MessagingCrawlListener;
import org.gbif.crawler.xml.crawlserver.listener.ResultPersistingListener;

import java.io.File;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;

class XmlCrawlConsumer extends CrawlConsumer {

  private static final int DEFAULT_CONCURRENT_CONNECTIONS = 2;
  private static final String CONCURRENT_CONNECTIONS_PROPERTY = "concurrentConnections";

  private final Optional<Long> minDelay;
  private final Optional<Long> maxDelay;
  private final File responseArchive;

  /**
   * @param minDelay is the "grace" period in milliseconds we allow between calls to a URL implemented by waiting to
   *                 release the lock on the URL for this amount of time
   */
  XmlCrawlConsumer(CuratorFramework curator, MessagePublisher publisher, Optional<Long> minDelay,
    Optional<Long> maxDelay, File responseArchive) {
    super(curator, publisher);
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
    this.responseArchive = responseArchive;
  }

  @Override
  protected void crawl(UUID datasetKey, CrawlJob crawlJob) {
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

    crawler.addListener(
      new CrawlerZooKeeperUpdatingListener<ScientificNameRangeCrawlContext>(builder.getCrawlConfiguration(), curator));
    crawler.addListener(new LoggingCrawlListener<ScientificNameRangeCrawlContext>(builder.getCrawlConfiguration()));
    crawler.addListener(
      new MessagingCrawlListener<ScientificNameRangeCrawlContext>(publisher, builder.getCrawlConfiguration()));
    if (responseArchive != null) {
      crawler.addListener(new ResultPersistingListener(responseArchive, builder.getCrawlConfiguration()));
    }

    // This blocks until the crawl is finished.
    crawler.crawl();

    // Note that zookeeper finish updates are handled by the CrawlerZooKeeperUpdatingListener!
  }
}
