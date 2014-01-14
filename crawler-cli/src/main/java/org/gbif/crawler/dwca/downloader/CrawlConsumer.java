package org.gbif.crawler.dwca.downloader;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.getCrawlInfoPath;

/**
 * Consumer of the crawler queue that runs the actual dwc archive download and emits a DwcaDownloadFinishedMessage
 * when done.
 */
public class CrawlConsumer implements QueueConsumer<UUID> {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlConsumer.class);
  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new GuavaModule());
  }

  private final File archiveRepository;
  private final Counter startedDownloads = Metrics.newCounter(DownloaderService.class, "startedDownloads");
  private final Counter failedDownloads = Metrics.newCounter(DownloaderService.class, "failedDownloads");
  private final Counter notModified = Metrics.newCounter(DownloaderService.class, "notModified");
  private final HttpUtil client = new HttpUtil(HttpUtil.newMultithreadedClient(10 * 60 * 1000, 20, 2));
  private final CuratorFramework curator;
  private final MessagePublisher publisher;
  private final RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 10);

  public CrawlConsumer(CuratorFramework curator, MessagePublisher publisher, File archiveRepository) {
    this.curator = curator;
    this.publisher = publisher;
    this.archiveRepository = archiveRepository;
    if (!archiveRepository.exists() && !archiveRepository.isDirectory()) {
      throw new IllegalArgumentException("Archive repository needs to be an existing directory: "
                                         + archiveRepository.getAbsolutePath());
    }
    if (!archiveRepository.canWrite()) {
      throw new IllegalArgumentException("Archive repository directory not writable: "
                                         + archiveRepository.getAbsolutePath());
    }
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    LOG.info("State changed to [{}]", newState);
  }

  @Override
  public void consumeMessage(UUID message) throws Exception {
    LOG.debug("Got crawl job for UUID [{}]", message);
    DwcaDownloadFinishedMessage finishedMessage;
    try {
      byte[] bytes = curator.getData().forPath(getCrawlInfoPath(message));
      CrawlJob crawlJob = MAPPER.readValue(bytes, CrawlJob.class);

      startCrawl(crawlJob.getDatasetKey());
      MDC.put("datasetKey", message.toString());
      MDC.put("attempt", String.valueOf(crawlJob.getAttempt()));
      finishedMessage = doCrawl(crawlJob);
    } catch (RuntimeException e) {
      createOrUpdateZooKeeper(getCrawlInfoPath(message, FINISHED_REASON),
                              FinishReason.ABORT.toString().getBytes(Charsets.UTF_8));
      LOG.warn("Finished failed download attempt of DwC-A for dataset [{}]", message);
      // TODO: Send a message on failure, see CRAWLER-72
      return;
    } finally {
      MDC.remove("datasetKey");
      MDC.remove("attempt");
      updateDate(message, CrawlerNodePaths.FINISHED_CRAWLING);
    }

    // This will only be reached when the crawl was successful
    createOrUpdateZooKeeper(getCrawlInfoPath(message, FINISHED_REASON),
                            FinishReason.NORMAL.toString().getBytes(Charsets.UTF_8));
    DistributedAtomicLong dal =
      new DistributedAtomicLong(curator, getCrawlInfoPath(message, CrawlerNodePaths.PAGES_CRAWLED), retryPolicy);
    dal.trySet(1L);

    // If the archive wasn't modified we are done processing so we need to update ZooKeeper to reflect this
    if (!finishedMessage.isModified()) {
      dal = new DistributedAtomicLong(curator,
                                      getCrawlInfoPath(message, CrawlerNodePaths.PAGES_FRAGMENTED_SUCCESSFUL),
                                      retryPolicy);
      dal.trySet(1L);
    }
    LOG.debug("Finished download attempt of DwC-A for dataset [{}]", message);
  }

  /**
   * The started crawl is declared in zookeeper by putting the date into
   * /crawls/UUID/startedCrawling
   */
  private void startCrawl(UUID datasetKey) {
    updateDate(datasetKey, CrawlerNodePaths.STARTED_CRAWLING);
  }

  /**
   * Updates a node in ZooKeeper saving the current date in time in there.
   *
   * @param datasetKey designates the first bit of the path to update
   * @param path        the path to update within the dataset node
   */
  private void updateDate(UUID datasetKey, String path) {
    String crawlPath = getCrawlInfoPath(datasetKey, path);
    Date date = new Date();

    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    byte[] data = dateFormat.format(date).getBytes(Charsets.UTF_8);
    createOrUpdateZooKeeper(crawlPath, data);
  }

  private void createOrUpdateZooKeeper(String path, byte[] data) {
    try {
      Stat stat = curator.checkExists().forPath(path);
      if (stat == null) {
        curator.create().forPath(path, data);
      } else {
        curator.setData().forPath(path, data);
      }
    } catch (Exception e) {
      LOG.warn("Exception while updating ZooKeeper", e);
    }
  }

  private DwcaDownloadFinishedMessage doCrawl(CrawlJob crawlRequest) {
    Preconditions.checkNotNull("crawlRequest required", crawlRequest);
    UUID uuid = crawlRequest.getDatasetKey();

    startedDownloads.inc();

    // we keep the compressed file forever and use it to retrieve the last modified for conditional gets
    File localFile = new File(archiveRepository, uuid + ".dwca");
    // the decompressed archive folder is created by this consumer and then gets deleted at the very end of the processing
    File archiveDir = new File(archiveRepository, uuid.toString());
    if (archiveDir.exists()) {
      // clean up any existing folder
      LOG.debug("Deleting existing archive folder [{}]", archiveDir.getAbsolutePath());
      FileUtils.deleteDirectoryRecursively(archiveDir);
    }

    boolean modified = false;
    Date lastModified = null;

    try {
      StatusLine status = client.downloadIfModifiedSince(crawlRequest.getTargetUrl().toURL(), localFile);

      if (status.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
        notModified.inc();
        // use last modified from previous file
        lastModified = new Date(localFile.lastModified());

      } else if (HttpUtil.success(status)) {
        modified = true;
        lastModified = new Date();
        try {
          CompressionUtil.decompressFile(archiveDir, localFile, true);
        } catch (CompressionUtil.UnsupportedCompressionType unsupportedCompressionType) {
          LOG.debug("Could not uncompress archive for dataset [{}]", uuid, unsupportedCompressionType);
          try {
            ArchiveFactory.openArchive(localFile);
            LOG.debug("Was able to read plain text file for dataset [{}]", uuid);
            if (!archiveDir.mkdir()) {
              LOG.error("Failed creating directory [{}], aborting dataset [{}]", archiveDir, uuid);
              return null;
            }
            Files.copy(localFile, new File(archiveDir, Files.getNameWithoutExtension(localFile.getName()) + ".csv"));
          } catch (UnsupportedArchiveException ignored) {
            LOG.warn("Not a valid archive or plain text file for dataset [{}]", uuid);
            failedDownloads.inc();
            throw new IllegalStateException("Was able to download but received invalid archive for dataset "
                                            + "[" + uuid + "]");
          }
        }
      } else {
        failedDownloads.inc();
        throw new IllegalStateException("HTTP " + status.getStatusCode() +
                                        ". Failed to download DwC-A for dataset " + uuid +
                                        " from " + crawlRequest.getTargetUrl());
      }

    } catch (IOException e) {
      failedDownloads.inc();
      LOG.error("Failed to download dwc archive for dataset [{}] from [{}]",
                crawlRequest.getDatasetKey(),
                crawlRequest.getTargetUrl(),
                e);
      throw new RuntimeException(e);
    }

    LOG.info("Successfully downloaded DwC-A for dataset, modified [{}], lastModified [{}], uuid [{}]",
             modified,
             lastModified,
             uuid);

    // send download success message
    try {
      DwcaDownloadFinishedMessage message = new DwcaDownloadFinishedMessage(crawlRequest.getDatasetKey(),
                                                                            crawlRequest.getTargetUrl(),
                                                                            crawlRequest.getAttempt(),
                                                                            lastModified,
                                                                            modified);
      publisher.send(message);
      return message;
    } catch (IOException e) {
      LOG.error("Failed to send download finished message for crawl [{}]", crawlRequest.getDatasetKey(), e);
    }
    return null;
  }

}
