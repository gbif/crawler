package org.gbif.crawler.abcda.downloader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.abcda.AbcdaConfiguration;
import org.gbif.crawler.common.crawlserver.CrawlServerBaseService;

import java.util.UUID;

import static org.gbif.crawler.constants.CrawlerNodePaths.*;

/**
 * This server watches a queue in ZooKeeper and processes each item which should represent a request to download an ABCD
 * archive.
 */
public class DownloaderService extends CrawlServerBaseService<AbcdaConfiguration> {

  public DownloaderService(AbcdaConfiguration config) {
    super(buildPath(ABCDA_CRAWL, QUEUED_CRAWLS), buildPath(ABCDA_CRAWL, RUNNING_CRAWLS), config);
  }

  @Override
  protected QueueConsumer<UUID> newConsumer(CuratorFramework curator, MessagePublisher publisher,
    AbcdaConfiguration config) {
    return new AbcdaCrawlConsumer(curator, publisher, config.archiveRepository);
  }

}
