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
package org.gbif.crawler.dwcdp.downloader;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.common.crawlserver.CrawlServerBaseService;

import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;

import org.gbif.crawler.dwcdp.DwcDpConfiguration;

import static org.gbif.crawler.constants.CrawlerNodePaths.DWC_DP_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.QUEUED_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.RUNNING_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.buildPath;

/**
 * This server watches a queue in ZooKeeper and processes each item which should represent a request
 * to download a DwcDP archive.
 */
public class DownloaderService extends CrawlServerBaseService<DwcDpConfiguration> {

  public DownloaderService(DwcDpConfiguration config) {
    super(buildPath(DWC_DP_CRAWL, QUEUED_CRAWLS), buildPath(DWC_DP_CRAWL, RUNNING_CRAWLS), config);
  }

  @Override
  protected QueueConsumer<UUID> newConsumer(
      CuratorFramework curator, MessagePublisher publisher, DwcDpConfiguration config) {
    return new DwcDpCrawlConsumer(curator, publisher, config.archiveRepository, config.httpTimeout);
  }
}
