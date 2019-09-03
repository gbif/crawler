package org.gbif.crawler.dwca.fragmenter;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaMetasyncFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.OccurrenceFragmentedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.dwca.DwcaService;
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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.common.ZookeeperUtils.getCounter;
import static org.gbif.crawler.common.ZookeeperUtils.updateCounter;
import static org.gbif.crawler.constants.CrawlerNodePaths.*;

/**
 * This service will listen for {@link DwcaValidationFinishedMessage} from RabbitMQ and for every valid archive it will
 * extract all {@link StarRecord} and hand them back to RabbitMQ as {@link OccurrenceFragmentedMessage}s.
 * While it does this it'll update appropriate counters in ZooKeeper.
 */
public class DwcaFragmenterService extends DwcaService {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaFragmenterService.class);
  private final DwcaFragmenterConfiguration cfg;
  private CuratorFramework curator;

  public DwcaFragmenterService(DwcaFragmenterConfiguration configuration) {
    super(configuration);
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

    super.startUp();
  }

  @Override
  protected void bindListeners() throws IOException {
    curator = cfg.zooKeeper.getCuratorFramework();

    listener.listen("dwca-fragmenter", cfg.poolSize,
        new DwCaCallback(datasetService, publisher, cfg.archiveRepository));
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
    private final DatasetService datasetService;

    private DwCaCallback(DatasetService datasetService, MessagePublisher publisher, File archiveRepository) {
      this.publisher = publisher;
      this.archiveRepository = archiveRepository;
      this.datasetService = datasetService;
    }

    @Override
    public void handleMessage(DwcaMetasyncFinishedMessage message) {
      UUID datasetKey = message.getDatasetUuid();
      MDC.put("datasetKey", datasetKey.toString());

      if (Platform.OCCURRENCE.equivalent(message.getPlatform())) {
        return;
      }

      if (message.getDatasetType() == DatasetType.METADATA) {
        // for metadata only dataset this is the last step
        // explicitly declare that no content is expected so the CoordinatorCleanup can pick it up.
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
          createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
          return;
        }

        int counter = 0;
        if (message.getDatasetType() == DatasetType.OCCURRENCE) {
          // check license
          if (licenseIsSupported(datasetKey)) {
            counter = handleOccurrenceCore(datasetKey, archive, message);
          } else {
            LOG.error("Refusing to fragment occurrence core in dataset [{}] with unsupported license", datasetKey);
            updateCounter(curator, datasetKey, PAGES_FRAGMENTED_ERROR, 1L);
            createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
            createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
            return;
          }
        } else if (archive.getExtension(DwcTerm.Occurrence) != null) {
          // e.g. Sample and Taxon archives can have occurrences

          // check license
          if (licenseIsSupported(datasetKey)) {
            counter = handleOccurrenceExtension(datasetKey, archive, DwcTerm.Occurrence, message);
          } else {
            LOG.error("Refusing to fragment occurrence extension in dataset [{}] with unsupported license", datasetKey);
            updateCounter(curator, datasetKey, PAGES_FRAGMENTED_ERROR, 1L);
            createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
            createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
            return;
          }
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

    /**
     * Check if the dataset's license is acceptable for us to process occurrences
     */
    private boolean licenseIsSupported(UUID datasetKey) {
      Dataset dataset = datasetService.get(datasetKey);
      if (dataset == null) {
        LOG.error("Dataset {} not found when checking license", datasetKey);
        return false;
      }

      return dataset.getLicense().isConcrete();
    }
  }
}
