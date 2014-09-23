package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.crawler.AbstractCrawlListener;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.io.IOException;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.common.ZookeeperUtils.updateDate;
import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_CONTEXT;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_CRAWLED;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.STARTED_CRAWLING;

@NotThreadSafe
public class CrawlerZooKeeperUpdatingListener<CTX extends CrawlContext>
  extends AbstractCrawlListener<CTX, String, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlerZooKeeperUpdatingListener.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new GuavaModule());
  }

  private final CrawlConfiguration configuration;
  private final CuratorFramework curator;

  public CrawlerZooKeeperUpdatingListener(CrawlConfiguration configuration, CuratorFramework curator) {
    this.configuration = configuration;
    this.curator = curator;
  }

  @Override
  public void startCrawl() {
    updateDate(curator, configuration.getDatasetKey(), STARTED_CRAWLING);
    // xml crawls are never for checklists, so set state to empty
    createOrUpdate(curator, configuration.getDatasetKey(), PROCESS_STATE_CHECKLIST, ProcessState.EMPTY);
    createOrUpdate(curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.RUNNING);
  }

  @Override
  public void progress(CTX context) {
    String crawlPath = CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), CRAWL_CONTEXT);
    try {
      byte[] data = MAPPER.writeValueAsBytes(context);
      createOrUpdate(curator, crawlPath, data);
    } catch (IOException e) {
      LOG.error("Error serializing context object", e);
    }
  }

  @Override
  public void response(List<Byte> response, int retry, long duration, Optional<Integer> recordCount,
    Optional<Boolean> endOfRecords) {
    String crawlPath = CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), PAGES_CRAWLED);
    DistributedAtomicLong dal = new DistributedAtomicLong(curator, crawlPath, new RetryNTimes(1, 1000));
    try {
      AtomicValue<Long> value = dal.increment();
      if (value.succeeded()) {
        LOG.debug("Set counter of pages crawled for [{}] to [{}]", configuration.getDatasetKey(), value.postValue());
      } else {
        LOG.error("Failed to update counter of pages crawled for [{}]", configuration.getDatasetKey(),
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
    createOrUpdate(curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
  }

  @Override
  public void finishCrawlAbnormally() {
    finishCrawl(FinishReason.ABORT);
    createOrUpdate(curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
  }

  public void finishCrawl(FinishReason reason) {
    updateDate(curator, configuration.getDatasetKey(), FINISHED_CRAWLING);
    createOrUpdate(curator, configuration.getDatasetKey(), FINISHED_REASON, reason);
  }

}
