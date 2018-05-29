package org.gbif.crawler.abcda.pager;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.AbcdaDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.CrawlResponseMessage;
import org.gbif.crawler.abcda.AbcdaConfiguration;
import org.gbif.utils.file.CompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.gbif.crawler.common.ZookeeperUtils.getCounter;
import static org.gbif.crawler.constants.CrawlerNodePaths.*;

/**
 * This service will listen for {@link AbcdaDownloadFinishedMessage} from RabbitMQ and for every valid archive it will
 * send the response XML file contents to Rabbit as {@link CrawlResponseMessage}s, as if from a BioCASe crawl.
 * While it does this it'll update appropriate counters in ZooKeeper.
 */
public class AbcdaPagerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AbcdaPagerService.class);
  private final AbcdaConfiguration cfg;
  private MessagePublisher publisher;
  private MessageListener listener;
  private CuratorFramework curator;

  public AbcdaPagerService(AbcdaConfiguration configuration) {
    this.cfg = configuration;
  }

  @Override
  protected void startUp() throws Exception {

    if (!cfg.archiveRepository.exists() && !cfg.archiveRepository.isDirectory()) {
      throw new IllegalArgumentException(
          "Archive repository needs to be an existing directory: " + cfg.archiveRepository.getAbsolutePath());
    }
    if (!cfg.archiveRepository.canWrite()) {
      throw new IllegalArgumentException(
          "Archive repository directory not writable: " + cfg.archiveRepository.getAbsolutePath());
    }

    curator = cfg.zooKeeper.getCuratorFramework();

    publisher = new DefaultMessagePublisher(cfg.messaging.getConnectionParameters());
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(cfg.messaging.getConnectionParameters(), 1);
    listener.listen("abcda-pager", cfg.poolSize, new AbcdaCallback(publisher, cfg.archiveRepository));
  }

  @Override
  protected void shutDown() throws Exception {
    publisher.close();
    listener.close();
    curator.close();
  }

  /**
   * This callback handles {@link AbcdaDownloadFinishedMessage}s and extracts XML responses out of the archives
   * and sends them on as messages.
   */
  private class AbcdaCallback extends AbstractMessageCallback<AbcdaDownloadFinishedMessage> {

    private final MessagePublisher publisher;
    private final File archiveRepository;

    private AbcdaCallback(MessagePublisher publisher, File archiveRepository) {
      this.publisher = publisher;
      this.archiveRepository = archiveRepository;
    }

    @Override
    public void handleMessage(AbcdaDownloadFinishedMessage message) {
      UUID datasetKey = message.getDatasetUuid();
      MDC.put("datasetKey", datasetKey.toString());

      File[] responses;
      try {
        responses = uncompress(
            new File(archiveRepository, datasetKey.toString() + AbcdaConfiguration.ABCDA_SUFFIX).toPath(),
            new File(archiveRepository, datasetKey.toString()).toPath());
      } catch (IOException ioEx) {
        LOG.error("Could not open archive for dataset [{}] : {}", datasetKey, ioEx.getMessage());
        return;
      }

      handleXmlResponses(datasetKey, message, responses);

      MDC.remove("datasetKey");
    }

    private File[] uncompress(Path abcdaLocation, Path destination) throws IOException {
      if (!Files.exists(abcdaLocation)) {
        throw new FileNotFoundException("abcdaLocation does not exist: " + abcdaLocation.toAbsolutePath());
      }

      if (Files.exists(destination)) {
        // clean up any existing folder
        LOG.debug("Deleting existing archive folder [{}]", destination.toAbsolutePath());
        org.gbif.utils.file.FileUtils.deleteDirectoryRecursively(destination.toFile());
      }
      FileUtils.forceMkdir(destination.toFile());
      // try to decompress archive
      try {
        CompressionUtil.decompressFile(destination.toFile(), abcdaLocation.toFile(), false);

        File[] rootFiles = destination.toFile().listFiles((FileFilter) HiddenFileFilter.VISIBLE);
        return rootFiles;
      } catch (CompressionUtil.UnsupportedCompressionType e) {
        throw new IOException(e);
      }
    }

    private void handleXmlResponses(UUID datasetKey, AbcdaDownloadFinishedMessage message, File[] responses) {
      LOG.info("Paging ABCD-A for dataset [{}]", datasetKey);

      int counter = 0;
      final DistributedAtomicLong zCounter = getCounter(curator, datasetKey, PAGES_CRAWLED);

      for (File f : responses) {
        try {
          try {
            sendXmlPage(Files.readAllBytes(f.toPath()), message);
            counter++;
          } catch (IOException ioEx) {
            LOG.error("Could not send message for dataset [{}] : {}", datasetKey, ioEx.getMessage());
          }
          incrementCounter(datasetKey, zCounter, 1L);
          LOG.info("Successfully extracted [{}] response pages out of ABCD-A for dataset [{}]", counter, datasetKey);
        } catch (Exception e) {
          LOG.error("Error iterating ABCD-A for dataset [{}]", datasetKey, e);
        }
      }
    }

    private void sendXmlPage(byte[] serializedRecord, AbcdaDownloadFinishedMessage message) throws IOException {
      publisher.send(new CrawlResponseMessage(message.getDatasetUuid(), message.getAttempt(), 1, serializedRecord,
          1, Optional.absent(), ""));
    }

    private void incrementCounter(UUID datasetKey, DistributedAtomicLong counter, long count) {
      try {
        if (count > 0) {
          AtomicValue<Long> value = counter.add(count);
          if (value.succeeded()) {
            LOG.debug("Set counter of pages crawled for [{}] to [{}]", datasetKey, value.postValue());
          } else {
            LOG.warn("Failed to update counter of pages crawled for [{}]", datasetKey, value.postValue());
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to update counter of pages crawled for [{}]", datasetKey, e);
      }
    }
  }
}
