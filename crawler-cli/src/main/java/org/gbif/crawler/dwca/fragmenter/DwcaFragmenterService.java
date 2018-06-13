package org.gbif.crawler.dwca.fragmenter;

import org.gbif.api.model.crawler.ProcessState;
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
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.Archive;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
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
import org.slf4j.MDC;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.common.ZookeeperUtils.getCounter;
import static org.gbif.crawler.common.ZookeeperUtils.updateCounter;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_EMITTED;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_SUCCESSFUL;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;

/**
 * This service will listen for {@link DwcaValidationFinishedMessage} from RabbitMQ and for every valid archive it will
 * extract all {@link StarRecord} and hand them back to RabbitMQ as {@link OccurrenceFragmentedMessage}s.
 * While it does this it'll update appropriate counters in ZooKeeper.
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
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(cfg.messaging.getConnectionParameters(), 1);
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
      MDC.put("datasetKey", datasetKey.toString());

      if (message.getDatasetType() == DatasetType.METADATA) {
        // for metadata only dataset this is the last step
        // explicitly declare that no content is expected so the CoordinatorCleanup can pick it
        createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.EMPTY);
        createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.EMPTY);
        createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.EMPTY);
        LOG.info("Marked metadata-only dataset as empty [{}]", datasetKey);

      } else {

        Archive archive;
        try {
          archive = DwcFiles.fromLocation(new File(archiveRepository, datasetKey.toString()).toPath());
        } catch (IOException ioEx) {
          LOG.error("Could not open archive for dataset [{}] : {}", datasetKey, ioEx.getMessage());
          updateCounter(curator, datasetKey, PAGES_FRAGMENTED_ERROR, 1L);
          return;
        }

        int counter = -1;
        if (message.getDatasetType() == DatasetType.OCCURRENCE) {
          counter = handleOccurrenceCore(datasetKey, archive, message);
        } else if (archive.getExtension(DwcTerm.Occurrence) != null) {
          // e.g. Sample and Taxon archives can have occurrences
          counter = handleOccurrenceExtension(datasetKey, archive, DwcTerm.Occurrence, message);
        } else {
          LOG.info("Ignoring DwC-A for dataset [{}] because it does not have Occurrence information.",
            message.getDatasetUuid());
        }

        // Mark empty archives, this will be the end of processing.
        if (counter == 0) {
          createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.EMPTY);
        }
      }

      updateCounter(curator, datasetKey, PAGES_FRAGMENTED_SUCCESSFUL, 1L);
      MDC.remove("datasetKey");
    }

    private int handleOccurrenceCore(UUID datasetKey, Archive archive, DwcaMetasyncFinishedMessage message) {
      LOG.info("Fragmenting DwC-A for dataset [{}] with occurrence core", datasetKey);

      int counter = 0;
      final DistributedAtomicLong zCounter = getCounter(curator, datasetKey, FRAGMENTS_EMITTED);

      try {
        //this can take some time if the archive includes extension(s)
        archive.initialize();
      } catch (IOException e) {
        LOG.error("Error extracting DwC-A for dataset [{}]", datasetKey);
        return -1;
      }

      try (ClosableIterator<StarRecord> iterator = archive.iterator(false, false)) {
        while (iterator.hasNext()) {
          StarRecord record = iterator.next();
          counter++;

          try {
            sendOccurrenceRecord(StarRecordSerializer.toJson(record), message);
          } catch (IOException ioEx) {
            LOG.error("Could not send message for dataset [{}] : {}", datasetKey, ioEx.getMessage());
          }
          if (counter % COUNTER_UPDATE_FREQUENCY == 0) {
            incrementCounter(datasetKey, zCounter, COUNTER_UPDATE_FREQUENCY);
          }
        }
        incrementCounter(datasetKey, zCounter, counter % COUNTER_UPDATE_FREQUENCY);
        LOG.info("Successfully extracted [{}] records out of DwC-A for dataset [{}]", counter, datasetKey);
      } catch (Exception e) {
        LOG.error("Error iterating DwC-A for dataset [{}]", datasetKey, e);
      }

      return counter;
    }

    /**
     * Uses a nested iterator over the star and the occurrence extension to produce unique, flattened occurrence
     * fragments.
     */
    private int handleOccurrenceExtension(UUID datasetKey, Archive archive, Term rowType,
                                           DwcaMetasyncFinishedMessage message) {
      LOG.info("Fragmenting DwC-A for dataset [{}] with {} extension", datasetKey, rowType);

      try {
        //this can take some time if the archive includes extension(s)
        archive.initialize();
      } catch (IOException ioEx) {
        LOG.error("Error extracting DwC-A for dataset [{}] : {}", datasetKey, ioEx.getMessage());
        return -1;
      }

      int counter = 0;
      try (ClosableIterator<StarRecord> iterator = archive.iterator(false, false)) {
        final DistributedAtomicLong zCounter = getCounter(curator, datasetKey, FRAGMENTS_EMITTED);
        while (iterator.hasNext()) {
          StarRecord record = iterator.next();
          List<Record> records = record.extension(rowType);
          if (records != null) {
            for (Record rec : record.extension(rowType)) {
              counter++;
              try {
                sendOccurrenceRecord(StarRecordSerializer.toJson(record.core(), rec), message);
              } catch (IOException e) {
                LOG.error("Could not send message for dataset [{}] : {}", datasetKey, e.getMessage());
              }
              if (counter % COUNTER_UPDATE_FREQUENCY == 0) {
                incrementCounter(datasetKey, zCounter, COUNTER_UPDATE_FREQUENCY);
              }
            }
          }
        }
        incrementCounter(datasetKey, zCounter, counter % COUNTER_UPDATE_FREQUENCY);
        LOG.info("Successfully extracted [{}] records out of DwC-A for dataset [{}]", counter, datasetKey);

      } catch (Exception e) {
        LOG.error("Error iterating DwC-A for dataset [{}]", datasetKey);
      }

      return counter;
    }

    /**
     * Mostly to improve code readability.
     *
     * @param serializedRecord
     * @param message
     * @throws IOException
     */
    private void sendOccurrenceRecord(byte[] serializedRecord, DwcaMetasyncFinishedMessage message) throws IOException {
      publisher.send(
              new OccurrenceFragmentedMessage(message.getDatasetUuid(), message.getAttempt(), serializedRecord,
                      OccurrenceSchemaType.DWCA, EndpointType.DWC_ARCHIVE, message.getValidationReport()));
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
