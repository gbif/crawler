package org.gbif.crawler.common;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.util.UUID;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.common.ZookeeperUtils.updateDate;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;

public abstract class CrawlConsumer implements QueueConsumer<UUID> {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlConsumer.class);
  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected final CuratorFramework curator;
  protected final MessagePublisher publisher;

  static {
    MAPPER.registerModule(new GuavaModule());
  }

  public CrawlConsumer(CuratorFramework curator, MessagePublisher publisher) {
    this.curator = curator;
    this.publisher = publisher;
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    LOG.info("State changed to [{}]", newState);
  }

  protected abstract void crawl(UUID datasetKey, CrawlJob crawlJob) throws Exception;

  @Override
  public void consumeMessage(UUID message) throws Exception {
    LOG.debug("Got crawl job for UUID [{}]", message);
    MDC.put("datasetKey", message.toString());

    try {
      byte[] bytes = curator.getData().forPath(CrawlerNodePaths.getCrawlInfoPath(message));
      CrawlJob crawlJob = MAPPER.readValue(bytes, CrawlJob.class);
      LOG.debug("Crawl job detail [{}]", crawlJob);
      MDC.put("attempt", String.valueOf(crawlJob.getAttempt()));
      // execute crawl in subclass
      crawl(message, crawlJob);

    } catch (Exception e) {
      // If we catch an exception here that wasn't handled before then something bad happened and we'll abort
      // TODO: This is a dirty copy & paste job and should be replaced with a library but we don't have access to the
      // CrawlerZooKeeperUpdatingListener at this point.
      LOG.warn("Caught exception while crawling. Aborting crawl.", e);

      updateDate(curator, message, FINISHED_CRAWLING);
      createOrUpdate(curator, message, FINISHED_REASON, FinishReason.ABORT);
    } finally {
      LOG.debug("Finished crawling [{}]", message);
      MDC.remove("datasetKey");
      MDC.remove("attempt");
    }
  }

}
