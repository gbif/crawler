package org.gbif.crawler.coldp.downloader;

import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.coldp.ColDpConfiguration;
import org.gbif.crawler.common.crawlserver.CrawlServerBaseService;

import static org.gbif.crawler.constants.CrawlerNodePaths.COL_DP_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.QUEUED_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.RUNNING_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.buildPath;

public class DownloaderService extends CrawlServerBaseService<ColDpConfiguration> {

  public DownloaderService(ColDpConfiguration config) {
    super(buildPath(COL_DP_CRAWL, QUEUED_CRAWLS), buildPath(COL_DP_CRAWL, RUNNING_CRAWLS), config);
  }

  @Override
  protected QueueConsumer<UUID> newConsumer(
    CuratorFramework curator, MessagePublisher publisher, ColDpConfiguration config) {
    return new ColDpCrawlConsumer(curator, publisher, config.archiveRepository, config.httpTimeout);
  }

}
