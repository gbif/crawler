package org.gbif.crawler.xml.crawlserver;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.common.crawlserver.CrawlServerBaseService;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.util.UUID;

import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;

/**
 * The downloader service watches zookeeper
 */
public class XmlCrawlServerService extends CrawlServerBaseService<XmlCrawlServerConfiguration> {


  public XmlCrawlServerService(XmlCrawlServerConfiguration config) {
    super(CrawlerNodePaths.buildPath(CrawlerNodePaths.XML_CRAWL, CrawlerNodePaths.QUEUED_CRAWLS),
      CrawlerNodePaths.buildPath(CrawlerNodePaths.XML_CRAWL, CrawlerNodePaths.RUNNING_CRAWLS), config);
  }

  @Override
  protected QueueConsumer<UUID> newConsumer(CuratorFramework curator, MessagePublisher publisher,
    XmlCrawlServerConfiguration config) {
    return new XmlCrawlConsumer(curator, publisher, Optional.fromNullable(config.minLockDelay),
      Optional.fromNullable(config.maxLockDelay), config.responseArchive);
  }

}
