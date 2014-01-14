package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.crawler.AbstractCrawlListener;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import javax.annotation.concurrent.NotThreadSafe;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_CONTEXT;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_CRAWLED;
import static org.gbif.crawler.constants.CrawlerNodePaths.STARTED_CRAWLING;

@NotThreadSafe
public class CrawlerZooKeeperUpdatingListener<CTX extends CrawlContext>
  extends AbstractCrawlListener<CTX, String, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlerZooKeeperUpdatingListener.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new GuavaModule());
  }

  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  private final CrawlConfiguration configuration;
  private final CuratorFramework curator;

  public CrawlerZooKeeperUpdatingListener(CrawlConfiguration configuration, CuratorFramework curator) {
    this.configuration = configuration;
    this.curator = curator;

    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Override
  public void startCrawl() {
    String crawlPath = CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), STARTED_CRAWLING);
    Date date = new Date();
    byte[] data = dateFormat.format(date).getBytes(Charsets.UTF_8);
    createOrUpdateZooKeeper(crawlPath, data);
  }

  @Override
  public void progress(CTX context) {
    String crawlPath = CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), CRAWL_CONTEXT);
    try {
      byte[] data = MAPPER.writeValueAsBytes(context);
      createOrUpdateZooKeeper(crawlPath, data);
    } catch (IOException e) {
      LOG.error("Error serializing context object", e);
    }
  }

  @Override
  public void response(
    List<Byte> response, int retry, long duration, Optional<Integer> recordCount, Optional<Boolean> endOfRecords
  ) {
    String crawlPath = CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), PAGES_CRAWLED);
    DistributedAtomicLong dal = new DistributedAtomicLong(curator, crawlPath, new RetryNTimes(1, 1000));
    try {
      AtomicValue<Long> value = dal.increment();
      if (value.succeeded()) {
        LOG.debug("Set counter of pages crawled for [{}] to [{}]", configuration.getDatasetKey(), value.postValue());
      } else {
        LOG.error("Failed to update counter of pages crawled for [{}]",
                  configuration.getDatasetKey(),
                  value.postValue());
      }
    } catch (Exception e) {
      LOG.error("Failed to update counter of pages crawled for [{}]", configuration.getDatasetKey(), e);
    }
  }

  @Override
  public void finishCrawlNormally() {
    finishCrawl(FinishReason.NORMAL);
  }

  @Override
  public void finishCrawlOnUserRequest() {
    finishCrawl(FinishReason.USER_ABORT);
  }

  @Override
  public void finishCrawlAbnormally() {
    finishCrawl(FinishReason.ABORT);
  }

  public void finishCrawl(FinishReason reason) {
    String crawlPath = CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), FINISHED_CRAWLING);
    Date date = new Date();
    byte[] data = dateFormat.format(date).getBytes(Charsets.UTF_8);
    createOrUpdateZooKeeper(crawlPath, data);

    crawlPath = CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), FINISHED_REASON);
    createOrUpdateZooKeeper(crawlPath, reason.toString().getBytes(Charsets.UTF_8));
  }

  private void createOrUpdateZooKeeper(String path, byte[] data) {
    try {
      Stat stat = curator.checkExists().forPath(path);
      if (stat == null) {
        curator.create().forPath(path, data);
      } else {
        curator.setData().forPath(path, data);
      }
    } catch (Exception e) {
      LOG.error("Exception while updating ZooKeeper", e);
    }
  }

}
