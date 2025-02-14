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
package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.AbstractCrawlListener;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.common.ZookeeperUtils.updateDate;
import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_CONTEXT;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_CRAWLING;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_CRAWLED;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;
import static org.gbif.crawler.constants.CrawlerNodePaths.STARTED_CRAWLING;

@NotThreadSafe
public class CrawlerZooKeeperUpdatingListener<CTX extends CrawlContext>
    extends AbstractCrawlListener<CTX, String, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlerZooKeeperUpdatingListener.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private int totalRecordCount;

  static {
    MAPPER.registerModule(new GuavaModule());
    MAPPER.registerModule(new Jdk8Module()); // Support for Optional<Long>
  }

  private final CrawlConfiguration configuration;
  private final CuratorFramework curator;
  private final Platform platform;

  public CrawlerZooKeeperUpdatingListener(
      CrawlConfiguration configuration, CuratorFramework curator, Platform platform) {
    this.configuration = configuration;
    this.curator = curator;
    this.platform = platform;
  }

  @Override
  public void startCrawl() {
    updateDate(curator, configuration.getDatasetKey(), STARTED_CRAWLING);
    // xml crawls are never for checklists or sample based, so set state to empty
    createOrUpdate(
        curator, configuration.getDatasetKey(), PROCESS_STATE_CHECKLIST, ProcessState.EMPTY);
    createOrUpdate(
        curator, configuration.getDatasetKey(), PROCESS_STATE_SAMPLE, ProcessState.EMPTY);
    createOrUpdate(
        curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.RUNNING);
  }

  @Override
  public void progress(CTX context) {
    String crawlPath =
        CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), CRAWL_CONTEXT);
    try {
      byte[] data = MAPPER.writeValueAsBytes(context);
      createOrUpdate(curator, crawlPath, data);
    } catch (IOException e) {
      LOG.error("Error serializing context object", e);
    }
  }

  @Override
  public void response(
      List<Byte> response,
      int retry,
      long duration,
      Optional<Integer> recordCount,
      Optional<Boolean> endOfRecords) {
    totalRecordCount += recordCount.orElse(0);
    String crawlPath =
        CrawlerNodePaths.getCrawlInfoPath(configuration.getDatasetKey(), PAGES_CRAWLED);
    DistributedAtomicLong dal =
        new DistributedAtomicLong(curator, crawlPath, new RetryNTimes(1, 1000));
    try {
      AtomicValue<Long> value = dal.increment();
      if (value.succeeded()) {
        LOG.debug(
            "Set counter of pages crawled for [{}] to [{}]",
            configuration.getDatasetKey(),
            value.postValue());
      } else {
        LOG.error(
            "Failed to update counter of pages crawled for [{}] to [{}]",
            configuration.getDatasetKey(),
            value.postValue());
      }
    } catch (Exception e) {
      LOG.error(
          "Failed to update counter of pages crawled for [{}]", configuration.getDatasetKey(), e);
    }
  }

  @Override
  public void finishCrawlNormally() {
    finishCrawl(FinishReason.NORMAL);

    if (totalRecordCount == 0) {
      // Empty dataset, this is the end of processing.
      createOrUpdate(
          curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.EMPTY);
    } else {
      createOrUpdate(
        curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    }
  }

  @Override
  public void finishCrawlOnUserRequest() {
    finishCrawl(FinishReason.USER_ABORT);
    createOrUpdate(
        curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
  }

  @Override
  public void finishCrawlAbnormally() {
    finishCrawl(FinishReason.ABORT);
    createOrUpdate(
        curator, configuration.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
  }

  private void finishCrawl(FinishReason reason) {
    updateDate(curator, configuration.getDatasetKey(), FINISHED_CRAWLING);
    createOrUpdate(curator, configuration.getDatasetKey(), FINISHED_REASON, reason);
  }
}
