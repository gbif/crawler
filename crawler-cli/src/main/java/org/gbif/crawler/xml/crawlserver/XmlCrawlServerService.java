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
package org.gbif.crawler.xml.crawlserver;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.common.crawlserver.CrawlServerBaseService;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;

import com.google.common.base.Optional;

/** The downloader service watches zookeeper */
public class XmlCrawlServerService extends CrawlServerBaseService<XmlCrawlServerConfiguration> {

  public XmlCrawlServerService(XmlCrawlServerConfiguration config) {
    super(
        CrawlerNodePaths.buildPath(CrawlerNodePaths.XML_CRAWL, CrawlerNodePaths.QUEUED_CRAWLS),
        CrawlerNodePaths.buildPath(CrawlerNodePaths.XML_CRAWL, CrawlerNodePaths.RUNNING_CRAWLS),
        config);
  }

  @Override
  protected QueueConsumer<UUID> newConsumer(
      CuratorFramework curator, MessagePublisher publisher, XmlCrawlServerConfiguration config) {
    return new XmlCrawlConsumer(
        curator,
        publisher,
        Optional.fromNullable(config.minLockDelay),
        Optional.fromNullable(config.maxLockDelay),
        config.responseArchive);
  }
}
