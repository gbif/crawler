package org.gbif.crawler.coldp.downloader;

import java.io.File;
import java.util.Date;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.coldp.ColDpConfiguration;
import org.gbif.crawler.common.DownloadCrawlConsumer;

public class ColDpCrawlConsumer extends DownloadCrawlConsumer {

  public ColDpCrawlConsumer(
      CuratorFramework curator,
      MessagePublisher publisher,
      File archiveRepository,
      int httpTimeout) {
    super(curator, publisher, archiveRepository, httpTimeout);
  }

  @Override
  protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
    return new ColDpDownloadFinishedMessage(
      crawlJob.getDatasetKey(),
      crawlJob.getTargetUrl(),
      crawlJob.getAttempt(),
      new Date(),
      true,
      crawlJob.getEndpointType(),
      Platform.parseOrDefault(crawlJob.getProperty("platform"), Platform.ALL));
  }

  @Override
  protected String getSuffix() {
    return ColDpConfiguration.COLDP_SUFFIX;
  }

  @Override
  protected File getArchiveDirectory(File archiveRepository, UUID datasetKey) {
    return new File(archiveRepository, datasetKey.toString());
  }
}
