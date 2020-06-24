/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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
package org.gbif.crawler.common;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

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
  public void consumeMessage(UUID datasetKey) throws Exception {
    LOG.debug("Got crawl job for UUID [{}]", datasetKey);
    MDC.put("datasetKey", datasetKey.toString());

    try {
      byte[] bytes = curator.getData().forPath(CrawlerNodePaths.getCrawlInfoPath(datasetKey));
      CrawlJob crawlJob = MAPPER.readValue(bytes, CrawlJob.class);
      LOG.debug("Crawl job detail [{}]", crawlJob);
      MDC.put("attempt", String.valueOf(crawlJob.getAttempt()));
      // execute crawl in subclass
      crawl(datasetKey, crawlJob);

    } catch (Exception e) {
      // If we catch an exception here that wasn't handled before then something bad happened and
      // we'll abort
      // TODO: This is a dirty copy & paste job and should be replaced with a library but we don't
      // have access to the
      // CrawlerZooKeeperUpdatingListener at this point.
      LOG.warn("Caught exception while crawling. Aborting crawl: {}", e.getMessage());

      updateDate(curator, datasetKey, FINISHED_CRAWLING);
      createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);

    } finally {
      LOG.debug("Finished crawling [{}]", datasetKey);
      MDC.remove("datasetKey");
      MDC.remove("attempt");
    }
  }
}
