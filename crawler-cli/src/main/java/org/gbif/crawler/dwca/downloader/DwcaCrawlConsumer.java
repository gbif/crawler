package org.gbif.crawler.dwca.downloader;

import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.common.DownloadCrawlConsumer;
import org.gbif.crawler.dwca.DwcaConfiguration;

import java.io.File;
import java.util.Date;
import java.util.UUID;

import static org.gbif.crawler.common.ZookeeperUtils.updateCounter;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_CRAWLED;

/**
 * Consumer of the crawler queue that runs the actual dwc archive download and emits a DwcaDownloadFinishedMessage
 * when done.
 */
public class DwcaCrawlConsumer extends DownloadCrawlConsumer {

  public DwcaCrawlConsumer(CuratorFramework curator, MessagePublisher publisher, File archiveRepository) {
    super(curator, publisher, archiveRepository);
  }

  @Override
  protected void success(UUID datasetKey, CrawlJob crawlJob) {
    updateCounter(curator, datasetKey, PAGES_CRAWLED, 1l);
    super.success(datasetKey, crawlJob);
  }

  @Override
  protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
    return new DwcaDownloadFinishedMessage(crawlJob.getDatasetKey(),
                                           crawlJob.getTargetUrl(),
                                           crawlJob.getAttempt(),
                                           new Date(),
                                           true,
                                           crawlJob.getEndpointType(),
                                           Platform.parseOrDefault(crawlJob.getProperty("platform"), Platform.ALL));
  }

  @Override
  protected String getSuffix() {
    return DwcaConfiguration.DWCA_SUFFIX;
  }
}
