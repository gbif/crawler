package org.gbif.crawler.xml.crawlserver.builder;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.Crawler;
import org.gbif.crawler.CrawlerCoordinatorServiceImpl;
import org.gbif.crawler.RequestHandler;
import org.gbif.crawler.ResponseHandler;
import org.gbif.crawler.RetryPolicy;
import org.gbif.crawler.client.HttpCrawlClient;
import org.gbif.crawler.protocol.biocase.BiocaseCrawlConfiguration;
import org.gbif.crawler.protocol.biocase.BiocaseResponseHandler;
import org.gbif.crawler.protocol.biocase.BiocaseScientificNameRangeRequestHandler;
import org.gbif.crawler.protocol.digir.DigirCrawlConfiguration;
import org.gbif.crawler.protocol.digir.DigirResponseHandler;
import org.gbif.crawler.protocol.digir.DigirScientificNameRangeRequestHandler;
import org.gbif.crawler.protocol.tapir.TapirCrawlConfiguration;
import org.gbif.crawler.protocol.tapir.TapirResponseHandler;
import org.gbif.crawler.protocol.tapir.TapirScientificNameRangeRequestHandler;
import org.gbif.crawler.retry.LimitedRetryPolicy;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;
import org.gbif.crawler.strategy.ScientificNameRangeStrategy;
import org.gbif.crawler.xml.crawlserver.util.DynamicDelayingLockFactory;
import org.gbif.crawler.xml.crawlserver.util.HttpCrawlClientProvider;
import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.NoLockFactory;
import org.gbif.wrangler.lock.zookeeper.ZooKeeperLockFactory;

import java.util.List;
import javax.annotation.Nullable;

import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("ReturnOfThis")
public class CrawlerBuilder {

  private RequestHandler<ScientificNameRangeCrawlContext, String> requestHandler;
  private ResponseHandler<HttpResponse, List<Byte>> responseHandler;
  private CrawlConfiguration crawlConfiguration;
  private HttpCrawlClient client;
  private ScientificNameRangeStrategy strategy;
  private ScientificNameRangeCrawlContext context;
  private RetryPolicy retryPolicy;
  private LockFactory lockFactory;
  private Optional<Long> minDelay = Optional.absent();
  private Optional<Long> maxDelay = Optional.absent();
  private static final String LOCKING_PATH = "/lockedUrls/";


  public CrawlerBuilder(CrawlJob crawlJob) {
    setupProtocolSpecifics(crawlJob);
    setupCrawlClient();
  }

  public static CrawlerBuilder buildFor(CrawlJob crawlJob) {
    return new CrawlerBuilder(crawlJob);
  }

  public Crawler<ScientificNameRangeCrawlContext, String, HttpResponse, List<Byte>> build() {
    if (minDelay.isPresent()) {
      lockFactory = new DynamicDelayingLockFactory(lockFactory, minDelay.get(), maxDelay.get());
    }
    Lock lock = lockFactory.makeLock(crawlConfiguration.getUrl().getHost());
    return new Crawler<ScientificNameRangeCrawlContext, String, HttpResponse, List<Byte>>(strategy, requestHandler,
      responseHandler, client, retryPolicy, lock);
  }

  public CrawlConfiguration getCrawlConfiguration() {
    return crawlConfiguration;
  }

  public CrawlerBuilder withDefaultRetryStrategy() {
    retryPolicy = new LimitedRetryPolicy(10, 2, 10, 2);
    return this;
  }

  public CrawlerBuilder withDelayedLock(long minDelay, long maxDelay) {
    this.minDelay = Optional.of(minDelay);
    this.maxDelay = Optional.of(maxDelay);
    return this;
  }

  public CrawlerBuilder withoutLocking() {
    lockFactory = new NoLockFactory();
    return this;
  }

  public CrawlerBuilder withScientificNameRangeCrawlContext(ScientificNameRangeStrategy.Mode mode) {
    context = new ScientificNameRangeCrawlContext();
    strategy = new ScientificNameRangeStrategy(context, mode);
    return this;
  }

  public CrawlerBuilder withScientificNameRangeCrawlContext(@Nullable String lowerBound, @Nullable String upperBound) {
    context = new ScientificNameRangeCrawlContext(0, lowerBound, upperBound);
    return this;
  }


  public CrawlerBuilder withZooKeeperLock(CuratorFramework curator) {
    checkNotNull(curator, "curator can't be null");
    lockFactory = new ZooKeeperLockFactory(curator, LOCKING_PATH);
    return this;
  }

  public CrawlerBuilder withZooKeeperLock(CuratorFramework curator, int count) {
    checkNotNull(curator, "curator can't be null");
    lockFactory = new ZooKeeperLockFactory(curator, count, LOCKING_PATH);
    return this;
  }

  private void setupBiocaseCrawl(CrawlJob crawlJob) {
    checkNotNull(crawlJob.getProperty("conceptualSchema"));
    checkNotNull(crawlJob.getProperty("datasetTitle"));

    BiocaseCrawlConfiguration job =
      new BiocaseCrawlConfiguration(crawlJob.getDatasetKey(), crawlJob.getAttempt(), crawlJob.getTargetUrl(),
        crawlJob.getProperty("conceptualSchema"), crawlJob.getProperty("datasetTitle"));

    crawlConfiguration = job;
    requestHandler = new BiocaseScientificNameRangeRequestHandler(job);
    responseHandler = new BiocaseResponseHandler();
  }

  private void setupCrawlClient() {
    client = HttpCrawlClientProvider.newHttpCrawlClient();
  }

  private void setupDigirCrawl(CrawlJob crawlJob) {
    checkNotNull(crawlJob.getProperty("code"));
    checkNotNull(crawlJob.getProperty("manis"));

    DigirCrawlConfiguration job =
      new DigirCrawlConfiguration(crawlJob.getDatasetKey(), crawlJob.getAttempt(), crawlJob.getTargetUrl(),
        crawlJob.getProperty("code"), Boolean.valueOf(crawlJob.getProperty("manis")));

    crawlConfiguration = job;
    requestHandler = new DigirScientificNameRangeRequestHandler(job);
    responseHandler = new DigirResponseHandler();
  }

  private void setupProtocolSpecifics(CrawlJob crawlJob) {
    switch (crawlJob.getEndpointType()) {
      case TAPIR:
        setupTapirCrawl(crawlJob);
        break;
      case BIOCASE:
        setupBiocaseCrawl(crawlJob);
        break;
      case DIGIR:
      case DIGIR_MANIS:
        setupDigirCrawl(crawlJob);
        break;
      default:
        // LOG.warn("Received crawl for unsupported endpoint, [{}]", crawlJob);
    }
  }

  /**
   * This extracts the properties from the crawl job specific to the TAPIR crawl.
   * When used with the GBIF messaging service, this is populated in the private getCrawlJob() method in
   * {@link CrawlerCoordinatorServiceImpl}. Note that in that implementation it selects a single
   * <em>conceptualSchema</em> to use as the <em>contentNamespace</em> during crawling. Should logic be required to
   * change the selection criteria, it must happen when the crawlJob is created, such as in
   * {@link CrawlerCoordinatorServiceImpl}.
   */
  private void setupTapirCrawl(CrawlJob crawlJob) {
    checkNotNull(crawlJob.getProperty("conceptualSchema"));

    TapirCrawlConfiguration job =
      new TapirCrawlConfiguration(crawlJob.getDatasetKey(), crawlJob.getAttempt(), crawlJob.getTargetUrl(),
        crawlJob.getProperty("conceptualSchema"));

    crawlConfiguration = job;
    requestHandler = new TapirScientificNameRangeRequestHandler(job);
    responseHandler = new TapirResponseHandler();
  }

}
