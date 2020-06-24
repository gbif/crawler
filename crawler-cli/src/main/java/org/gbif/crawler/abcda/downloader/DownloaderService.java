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
package org.gbif.crawler.abcda.downloader;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.abcda.AbcdaConfiguration;
import org.gbif.crawler.common.crawlserver.CrawlServerBaseService;

import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;

import static org.gbif.crawler.constants.CrawlerNodePaths.*;

/**
 * This server watches a queue in ZooKeeper and processes each item which should represent a request
 * to download an ABCD archive.
 */
public class DownloaderService extends CrawlServerBaseService<AbcdaConfiguration> {

  public DownloaderService(AbcdaConfiguration config) {
    super(buildPath(ABCDA_CRAWL, QUEUED_CRAWLS), buildPath(ABCDA_CRAWL, RUNNING_CRAWLS), config);
  }

  @Override
  protected QueueConsumer<UUID> newConsumer(
      CuratorFramework curator, MessagePublisher publisher, AbcdaConfiguration config) {
    return new AbcdaCrawlConsumer(curator, publisher, config.archiveRepository, config.httpTimeout);
  }
}
