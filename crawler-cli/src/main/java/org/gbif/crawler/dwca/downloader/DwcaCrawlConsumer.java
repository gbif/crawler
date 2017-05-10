package org.gbif.crawler.dwca.downloader;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;
import org.gbif.crawler.common.CrawlConsumer;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.utils.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import static org.gbif.crawler.common.ZookeeperUtils.*;
import static org.gbif.crawler.constants.CrawlerNodePaths.*;

/**
 * Consumer of the crawler queue that runs the actual dwc archive download and emits a DwcaDownloadFinishedMessage
 * when done.
 */
public class DwcaCrawlConsumer extends CrawlConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaCrawlConsumer.class);
  public static final String DWCA_SUFFIX = ".dwca";

  private final File archiveRepository;
  private final Counter startedDownloads = Metrics.newCounter(DownloaderService.class, "startedDownloads");
  private final Counter failedDownloads = Metrics.newCounter(DownloaderService.class, "failedDownloads");
  private final Counter notModified = Metrics.newCounter(DownloaderService.class, "notModified");
  private final HttpUtil client = new HttpUtil(HttpUtil.newMultithreadedClient(10 * 60 * 1000, 25, 2));

  public DwcaCrawlConsumer(CuratorFramework curator, MessagePublisher publisher, File archiveRepository) {
    super(curator, publisher);
    this.archiveRepository = archiveRepository;
    if (!archiveRepository.exists() && !archiveRepository.isDirectory()) {
      throw new IllegalArgumentException(
        "Archive repository needs to be an existing directory: " + archiveRepository.getAbsolutePath());
    }
    if (!archiveRepository.canWrite()) {
      throw new IllegalArgumentException(
        "Archive repository directory not writable: " + archiveRepository.getAbsolutePath());
    }
  }

  @Override
  protected void crawl(UUID datasetKey, CrawlJob crawlJob) throws Exception {
    // The started crawl is declared in zookeeper by putting the date into
    // /crawls/UUID/startedCrawling
    updateDate(curator, datasetKey, CrawlerNodePaths.STARTED_CRAWLING);
    startedDownloads.inc();

    // we keep the compressed file forever and use it to retrieve the last modified for conditional gets
    final File localFile = new File(archiveRepository, datasetKey + DWCA_SUFFIX);

    try (MDC.MDCCloseable closeable = MDC.putCloseable("datasetKey", datasetKey.toString())) {
      LOG.info("Start download of DwC archive from {} to {}", crawlJob.getTargetUrl(), localFile);
      StatusLine status = client.downloadIfModifiedSince(crawlJob.getTargetUrl().toURL(), localFile);

      if (status.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
        notModified(datasetKey);

      } else if (HttpUtil.success(status)) {
        success(datasetKey, crawlJob);

      } else {
        failed(datasetKey);
        throw new IllegalStateException("HTTP " + status.getStatusCode() + ". Failed to download DwC-A for dataset "
                                        + datasetKey + " from " + crawlJob.getTargetUrl());
      }

    } catch (IOException e) {
      LOG.error("Failed to download DwC archive for dataset [{}] from [{}]", crawlJob.getDatasetKey(),
        crawlJob.getTargetUrl(), e);
      failed(datasetKey);
      throw new RuntimeException(e);

    } finally {
      // finished crawl
      updateDate(curator, datasetKey, CrawlerNodePaths.FINISHED_CRAWLING);
    }
  }

  private void failed(UUID datasetKey) {
    failedDownloads.inc();
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
    // we dont know the kind of dataset so we just put all states to finish
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  private void notModified(UUID datasetKey) {
    notModified.inc();
    LOG.info("DwC-A for dataset [{}] not modified. Crawl finished", datasetKey);
    // If the archive wasn't modified we are done processing so we need to update ZooKeeper to reflect this
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.NOT_MODIFIED);
    // we don't know the kind of dataset so we just put all states to finish
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  private void success(UUID datasetKey, CrawlJob crawlJob) {
    LOG.info("Successfully downloaded new DwC-A for dataset [{}]", datasetKey);
    updateCounter(curator, datasetKey, PAGES_CRAWLED, 1l);
    // send download success message
    try {
      DwcaDownloadFinishedMessage finishedMessage =
        new DwcaDownloadFinishedMessage(crawlJob.getDatasetKey(), crawlJob.getTargetUrl(), crawlJob.getAttempt(),
          new Date(), true);
      publisher.send(finishedMessage);
    } catch (IOException e) {
      LOG.error("Failed to send download finished message for crawl [{}]", crawlJob.getDatasetKey(), e);
    }
    // The crawl finished normally, processing still to run
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.NORMAL);
  }
}
