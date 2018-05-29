package org.gbif.crawler.abcda.downloader;

import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.AbcdaDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.crawler.abcda.AbcdaConfiguration;
import org.gbif.crawler.common.DownloadCrawlConsumer;

import java.io.File;
import java.util.Date;

/**
 * Consumer of the crawler queue that runs the actual ABCD archive download and emits a AbcdaDownloadFinishedMessage
 * when done.
 */
public class AbcdaCrawlConsumer extends DownloadCrawlConsumer {

  public AbcdaCrawlConsumer(CuratorFramework curator, MessagePublisher publisher, File archiveRepository) {
    super(curator, publisher, archiveRepository);
  }

  @Override
  protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
    return new AbcdaDownloadFinishedMessage(crawlJob.getDatasetKey(), crawlJob.getTargetUrl(), crawlJob.getAttempt(), new Date(), true);
  }

  @Override
  protected String getSuffix() {
    return AbcdaConfiguration.ABCDA_SUFFIX;
  }
}
