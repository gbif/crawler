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
package org.gbif.crawler.common;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.crawler.abcda.downloader.DownloaderService;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.utils.HttpClient;
import org.gbif.utils.HttpUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.common.ZookeeperUtils.updateDate;
import static org.gbif.crawler.constants.CrawlerNodePaths.*;

/**
 * Consumer of a crawler queue that runs the actual archive download and emits a message when done.
 */
public abstract class DownloadCrawlConsumer extends CrawlConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadCrawlConsumer.class);

  private final File archiveRepository;
  private final Counter startedDownloads =
      Metrics.newCounter(DownloaderService.class, "startedDownloads");
  private final Counter failedDownloads =
      Metrics.newCounter(DownloaderService.class, "failedDownloads");
  private final Counter notModified = Metrics.newCounter(DownloaderService.class, "notModified");
  private final HttpClient client;

  public DownloadCrawlConsumer(
      CuratorFramework curator,
      MessagePublisher publisher,
      File archiveRepository,
      int httpTimeout) {
    super(curator, publisher);
    this.archiveRepository = archiveRepository;
    if (!archiveRepository.exists() && !archiveRepository.isDirectory()) {
      throw new IllegalArgumentException(
          "Archive repository needs to be an existing directory: "
              + archiveRepository.getAbsolutePath());
    }
    if (!archiveRepository.canWrite()) {
      throw new IllegalArgumentException(
          "Archive repository directory not writable: " + archiveRepository.getAbsolutePath());
    }

    client = HttpUtil.newMultithreadedClient(httpTimeout, 25, 2);
  }

  @Override
  protected void crawl(UUID datasetKey, CrawlJob crawlJob) throws Exception {
    // The started crawl is declared in zookeeper by putting the date into
    // /crawls/UUID/startedCrawling
    updateDate(curator, datasetKey, CrawlerNodePaths.STARTED_CRAWLING);
    startedDownloads.inc();

    // DWCA downloaded archives are kept as archiveRepository/datasetKey/datasetKey.dwca and
    // datasetKey.attempt.dwca
    // ABCDA downloaded archives are kept as archiveRepository/datasetKey.abcda and
    // datasetKey.attempt.abcda
    final File datasetDirectory = getArchiveDirectory(archiveRepository, datasetKey);
    datasetDirectory.mkdirs();

    // we keep the file (potentially compressed) forever and use it to retrieve the last modified
    // for conditional gets
    final File localFile = new File(datasetDirectory, datasetKey + getSuffix());

    try (MDC.MDCCloseable ignored1 = MDC.putCloseable("datasetKey", datasetKey.toString());
        MDC.MDCCloseable ignored2 =
            MDC.putCloseable("attempt", String.valueOf(crawlJob.getAttempt()))) {
      // Sub-try so the MDC is still present for the exception logging.
      try {
        LOG.info("Start download of archive from {} to {}", crawlJob.getTargetUrl(), localFile);
        StatusLine status =
            client.downloadIfModifiedSince(crawlJob.getTargetUrl().toURL(), null, localFile, true);

        if (status.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
          notModified(datasetKey);
          Files.createLink(
              new File(datasetDirectory, datasetKey + "." + crawlJob.getAttempt() + getSuffix())
                  .toPath(),
              localFile.toPath());
        } else if (HttpUtil.success(status)) {
          success(datasetKey, crawlJob);
          Files.createLink(
              new File(datasetDirectory, datasetKey + "." + crawlJob.getAttempt() + getSuffix())
                  .toPath(),
              localFile.toPath());
        } else {
          failed(datasetKey);
          throw new IllegalStateException(
              "HTTP "
                  + status.getStatusCode()
                  + ". Failed to download archive for dataset "
                  + datasetKey
                  + " from "
                  + crawlJob.getTargetUrl());
        }
      } catch (IOException e) {
        LOG.error(
            "Failed to download archive for dataset [{}] from [{}]",
            crawlJob.getDatasetKey(),
            crawlJob.getTargetUrl(),
            e);
        failed(datasetKey);
        throw new RuntimeException(e);

      } finally {
        // finished crawl
        updateDate(curator, datasetKey, CrawlerNodePaths.FINISHED_CRAWLING);
      }
    }
  }

  protected void failed(UUID datasetKey) {
    failedDownloads.inc();
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
    // we don't know the kind of dataset so we just put all states to finish
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  protected void notModified(UUID datasetKey) {
    notModified.inc();
    LOG.info("Archive for dataset [{}] not modified. Crawl finished", datasetKey);
    // If the archive wasn't modified we are done processing so we need to update ZooKeeper to
    // reflect this
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.NOT_MODIFIED);
    // we don't know the kind of dataset so we just put all states to finish
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  protected void success(UUID datasetKey, CrawlJob crawlJob) {
    LOG.info("Successfully downloaded new archive for dataset [{}]", datasetKey);
    // send download success message
    try {
      publisher.send(createFinishedMessage(crawlJob));
    } catch (IOException e) {
      LOG.error(
          "Failed to send download finished message for crawl [{}]", crawlJob.getDatasetKey(), e);
    }
    // The crawl finished normally, processing still to run
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.NORMAL);
  }

  protected abstract DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob);

  protected abstract String getSuffix();

  protected abstract File getArchiveDirectory(File archiveRepository, UUID datasetKey);
}
