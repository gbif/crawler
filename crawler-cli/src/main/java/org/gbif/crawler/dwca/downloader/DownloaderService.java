package org.gbif.crawler.dwca.downloader;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.common.crawlserver.CrawlServerBaseService;

import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;

import static org.gbif.crawler.constants.CrawlerNodePaths.DWCA_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.QUEUED_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.RUNNING_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.buildPath;

/**
 * This server watches a queue in ZooKeeper and processes each item which should represent a request to download a DwC
 * archive.
 */
public class DownloaderService extends CrawlServerBaseService<DownloaderConfiguration> {

  public DownloaderService(DownloaderConfiguration config) {
    super(buildPath(DWCA_CRAWL, QUEUED_CRAWLS), buildPath(DWCA_CRAWL, RUNNING_CRAWLS), config);
  }

  @Override
  protected QueueConsumer<UUID> newConsumer(
    CuratorFramework curator, MessagePublisher publisher, DownloaderConfiguration config
  ) {
    return new CrawlConsumer(curator, publisher, config.archiveRepository);
  }

}
