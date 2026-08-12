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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.common.ZookeeperUtils.updateCounter;
import static org.gbif.crawler.common.ZookeeperUtils.updateDate;
import static org.gbif.crawler.constants.CrawlerNodePaths.*;

/**
 * Consumer of a crawler queue that runs the actual archive download and emits a message when done.
 */
public abstract class DownloadCrawlConsumer extends CrawlConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadCrawlConsumer.class);

  private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

  private final Counter startedDownloads =
    METRIC_REGISTRY.counter(MetricRegistry.name(DownloaderService.class, "startedDownloads"));
  private final Counter failedDownloads =
    METRIC_REGISTRY.counter(MetricRegistry.name(DownloaderService.class, "failedDownloads"));
  private final Counter notModified =
    METRIC_REGISTRY.counter(MetricRegistry.name(DownloaderService.class, "notModified"));

  private final File archiveRepository;

  private final HttpClient client;

  public DownloadCrawlConsumer(
    CuratorFramework curator,
    MessagePublisher publisher,
    File archiveRepository,
    int httpTimeout) {
    super(curator, publisher);
    this.archiveRepository = archiveRepository;
    if (!archiveRepository.exists() || !archiveRepository.isDirectory()) {
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
          if (linkAttemptFile(datasetDirectory, datasetKey, crawlJob, localFile)) {
            notModified(datasetKey);
          } else {
            duplicateAttempt(datasetKey, crawlJob);
          }
        } else if (HttpUtil.success(status)) {
          if (linkAttemptFile(datasetDirectory, datasetKey, crawlJob, localFile)) {
            afterSuccessfulDownload(datasetKey, crawlJob, localFile);
            success(datasetKey, crawlJob);
          } else {
            duplicateAttempt(datasetKey, crawlJob);
          }
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

  /**
   * Links the just-downloaded archive to its attempt-numbered file name (e.g.
   * {@code datasetKey.attempt.dwcdp}).
   *
   * @return true if a new link was created; false if the attempt file already existed, meaning
   *     this crawl attempt was already processed concurrently (e.g. a duplicate queue message,
   *     or two runs racing each other) - the caller must not proceed to trigger downstream
   *     processing again in that case, see {@link #duplicateAttempt(UUID, CrawlJob)}.
   */
  boolean linkAttemptFile(File datasetDirectory, UUID datasetKey, CrawlJob crawlJob, File localFile)
    throws IOException {
    File attemptFile =
      new File(datasetDirectory, datasetKey + "." + crawlJob.getAttempt() + getSuffix());
    try {
      Files.createLink(attemptFile.toPath(), localFile.toPath());
      return true;
    } catch (FileAlreadyExistsException e) {
      LOG.warn(
        "Attempt file [{}] for dataset [{}] attempt [{}] already exists - this attempt was "
        + "likely already processed concurrently (e.g. a duplicate queue message)",
        attemptFile, datasetKey, crawlJob.getAttempt());
      return false;
    }
  }

  /**
   * Called when this crawl attempt turns out to be a duplicate of one already processed
   * concurrently (see {@link #linkAttemptFile}). We still mark the crawl as finished in
   * ZooKeeper so nothing is left dangling, but deliberately do NOT call {@link #success} or
   * {@link #notModified}: both publish a "download finished" message, and the other, already-
   * completed run for this same attempt will (or already did) publish its own - sending a second
   * one here would cause this attempt to run through the full downstream pipeline twice.
   *
   * <p>We also deliberately do NOT write {@link CrawlerNodePaths#FINISHED_REASON} here: we can't
   * tell whether this run is racing ahead of or behind the genuine run for this attempt, so
   * writing e.g. {@code ABORT} risks overwriting the correct {@code NORMAL}/{@code NOT_MODIFIED}
   * the other run sets - in either order. Leaving it alone means whichever run actually completed
   * the real work gets to record why it finished.
   */
  protected void duplicateAttempt(UUID datasetKey, CrawlJob crawlJob) {
    LOG.warn(
      "Dataset [{}] attempt [{}] was already processed concurrently; marking finished without "
      + "triggering downstream processing again",
      datasetKey, crawlJob.getAttempt());
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  protected void failed(UUID datasetKey) {
    failedDownloads.inc();
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
    // we don't know the kind of dataset, so we just put all states to finish
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  protected void notModified(UUID datasetKey) {
    notModified.inc();
    LOG.info("Archive for dataset [{}] not modified. Crawl finished", datasetKey);
    // If the archive wasn't modified we are done processing, so we need to update ZooKeeper to
    // reflect this
    createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.NOT_MODIFIED);
    // we don't know the kind of dataset, so we just put all states to finish
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  /** For archive types we don't (yet) validate or otherwise process, the crawl process is now complete. */
  protected void finishedWithoutFurtherProcessing(UUID datasetKey) {
    LOG.info("Mark dataset [{}] as finished (no further processing here). Crawl finished", datasetKey);
    // we don't know the kind of dataset, so we just put all states to finish
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }

  protected void success(UUID datasetKey, CrawlJob crawlJob) {
    updateCounter(curator, datasetKey, PAGES_CRAWLED, 1L);
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

  /**
   * Hook for archive types that need extra work immediately after a successful download, before the
   * "download finished" message is published.
   */
  protected void afterSuccessfulDownload(UUID datasetKey, CrawlJob crawlJob, File localFile)
    throws IOException {}

  protected abstract String getSuffix();

  protected abstract File getArchiveDirectory(File archiveRepository, UUID datasetKey);
}
