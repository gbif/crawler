package org.gbif.crawler.dwca.fragmenter;

import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaMetasyncFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.OccurrenceFragmentedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.record.Record;
import org.gbif.dwca.record.StarRecord;
import org.gbif.utils.file.ClosableIterator;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.common.ZookeeperUtils.getCounter;
import static org.gbif.crawler.common.ZookeeperUtils.updateCounter;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_EMITTED;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_SUCCESSFUL;

/**
 * This service will listen for {@link DwcaValidationFinishedMessage} from RabbitMQ and for every valid archive it will
 * extract all {@link StarRecord} and hand them back to RabbitMQ as {@link OccurrenceFragmentedMessage}s. While it does
 * this it'll update appropriate counters in ZooKeeper.
 */
public class DwcaFragmenterService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaFragmenterService.class);
  private final DwcaFragmenterConfiguration cfg;
  private MessagePublisher publisher;
  private MessageListener listener;
  private CuratorFramework curator;

  public DwcaFragmenterService(DwcaFragmenterConfiguration configuration) {
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
    listener = new MessageListener(cfg.messaging.getConnectionParameters());
    listener.listen("dwca-fragmenter", cfg.poolSize,
      new DwCaCallback(publisher, cfg.archiveRepository));
  }

  @Override
  protected void shutDown() throws Exception {
    publisher.close();
    listener.close();
    curator.close();

  }

  /**
   * This callback handles {@link DwcaValidationFinishedMessage}s and extracts {@link StarRecord}s out of the archives
   * and sends them on as messages.
   */
  private class DwCaCallback extends AbstractMessageCallback<DwcaMetasyncFinishedMessage> {

    private static final int COUNTER_UPDATE_FREQUENCY = 1000;
    private final MessagePublisher publisher;
    private final File archiveRepository;

    private DwCaCallback(MessagePublisher publisher, File archiveRepository) {
      this.publisher = publisher;
      this.archiveRepository = archiveRepository;
    }

    @Override
    public void handleMessage(DwcaMetasyncFinishedMessage message) {
      UUID datasetKey = message.getDatasetUuid();

      Archive archive;
      try {
        archive = ArchiveFactory.openArchive(new File(archiveRepository, datasetKey.toString()));
      } catch (IOException e) {
        LOG.error("Could not open archive for dataset [{}]", datasetKey, e);
        updateCounter(curator, datasetKey, PAGES_FRAGMENTED_ERROR, 1l);
        return;
      }

      if (message.getDatasetType() == DatasetType.OCCURRENCE) {
        handleOccurrenceCore(datasetKey, archive, message);

      } else if (archive.getExtension(DwcTerm.Occurrence) != null) {
        handleOccurrenceExtension(datasetKey, archive, DwcTerm.Occurrence, message);

      } else {
        LOG.info("Ignoring DwC-A for dataset [{}] because it does not have Occurrence information.",
          message.getDatasetUuid());
      }
      updateCounter(curator, datasetKey, PAGES_FRAGMENTED_SUCCESSFUL, 1l);
    }

    private void handleOccurrenceCore(UUID datasetKey, Archive archive, DwcaMetasyncFinishedMessage message) {
      LOG.info("Fragmenting DwC-A for dataset [{}] with occurrence core", datasetKey);

      int counter = 0;
      final DistributedAtomicLong zCounter = getCounter(curator, datasetKey, FRAGMENTS_EMITTED);
      ClosableIterator<StarRecord> iterator = archive.iteratorRaw();
      while (iterator.hasNext()) {
        StarRecord record = iterator.next();
        counter++;

        byte[] serializedRecord = StarRecordSerializer.toJson(record);
        try {
          publisher.send(
            new OccurrenceFragmentedMessage(datasetKey, message.getAttempt(), serializedRecord, OccurrenceSchemaType.DWCA,
              EndpointType.DWC_ARCHIVE, message.getValidationReport()));
        } catch (IOException e) {
          LOG.error("Could not send message for dataset [{}]", datasetKey, e);
        }
        if (counter % COUNTER_UPDATE_FREQUENCY == 0) {
          incrementCounter(datasetKey, zCounter, COUNTER_UPDATE_FREQUENCY);
        }
      }
      incrementCounter(datasetKey, zCounter, counter % COUNTER_UPDATE_FREQUENCY);
      LOG.info("Successfully extracted [{}] records out of DwC-A for dataset [{}]", counter, datasetKey);
    }

    /**
     * Uses a nested iterator over the star and the occurrence extension to produce unique, falttened occurrence
     * fragments.
     */
    private void handleOccurrenceExtension(UUID datasetKey, Archive archive, Term rowType,
      DwcaMetasyncFinishedMessage message) {
      LOG.info("Fragmenting DwC-A for dataset [{}] with {} extension", datasetKey, rowType);

      ClosableIterator<StarRecord> iterator = archive.iteratorRaw();
      try {
        int counter = 0;
        final DistributedAtomicLong zCounter = getCounter(curator, datasetKey, FRAGMENTS_EMITTED);
        while (iterator.hasNext()) {
          StarRecord record = iterator.next();

          List<Record> records = record.extension(rowType);
          if (records != null) {
            for (Record rec : record.extension(rowType)) {
              counter++;
              byte[] serializedRecord = StarRecordSerializer.toJson(record.core(), rec);
              try {
                publisher.send(new OccurrenceFragmentedMessage(datasetKey, message.getAttempt(), serializedRecord,
                  OccurrenceSchemaType.DWCA, EndpointType.DWC_ARCHIVE, message.getValidationReport()));
              } catch (IOException e) {
                LOG.error("Could not send message for dataset [{}]", datasetKey, e);
              }
              if (counter % COUNTER_UPDATE_FREQUENCY == 0) {
                incrementCounter(datasetKey, zCounter, COUNTER_UPDATE_FREQUENCY);
              }
            }
          }
        }
        incrementCounter(datasetKey, zCounter, counter % COUNTER_UPDATE_FREQUENCY);
        LOG.info("Successfully extracted [{}] records out of DwC-A for dataset [{}]", counter, datasetKey);

      } finally {
        iterator.close();
      }
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
