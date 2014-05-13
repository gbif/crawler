package org.gbif.crawler.dwca.fragmenter;

import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.common.messaging.api.messages.DwcaMetasyncFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.OccurrenceFragmentedMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.dwca.LenientArchiveFactory;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.StarRecord;
import org.gbif.utils.file.ClosableIterator;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service will listen for {@link DwcaValidationFinishedMessage} from RabbitMQ and for every valid archive it will
 * extract all {@link StarRecord} and hand them back to RabbitMQ as {@link OccurrenceFragmentedMessage}s. While it does
 * this it'll update appropriate counters in ZooKeeper.
 */
public class DwcaFragmenterService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaFragmenterService.class);
  private final DwcaFragmenterConfiguration configuration;
  private MessagePublisher publisher;
  private MessageListener listener;
  private CuratorFramework curator;

  public DwcaFragmenterService(DwcaFragmenterConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {

    if (!configuration.archiveRepository.exists() && !configuration.archiveRepository.isDirectory()) {
      throw new IllegalArgumentException("Archive repository needs to be an existing directory: "
                                         + configuration.archiveRepository.getAbsolutePath());
    }
    if (!configuration.archiveRepository.canWrite()) {
      throw new IllegalArgumentException("Archive repository directory not writable: " + configuration.archiveRepository
        .getAbsolutePath());
    }

    // A cache for DistributedAtomicLongs to update ZooKeeper. NOTE: I'm not sure if this is actually worth it
    // TODO: Unhardcode the paths here in favor of CrawlerNodePaths
    LoadingCache<String, DistributedAtomicLong> cache =
      CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<String, DistributedAtomicLong>() {

        private final RetryPolicy retryPolicy = new RetryNTimes(5, 1000);

        @Override
        public DistributedAtomicLong load(String key) throws Exception {
          return new DistributedAtomicLong(curator, "/" + CrawlerNodePaths.CRAWL_INFO + "/" + key, retryPolicy);
        }
      });

    curator = configuration.zooKeeper.getCuratorFramework();

    publisher = new DefaultMessagePublisher(configuration.messaging.getConnectionParameters());
    listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.queueName,
                    configuration.poolSize,
                    new DwCaCallback(cache, publisher, configuration.archiveRepository));
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
  private static class DwCaCallback extends AbstractMessageCallback<DwcaMetasyncFinishedMessage> {

    private static final int COUNTER_UPDATE_FREQUENCY = 1000;
    private final LoadingCache<String, DistributedAtomicLong> counterCache;
    private final MessagePublisher publisher;
    private final File archiveRepository;

    private DwCaCallback(
      LoadingCache<String, DistributedAtomicLong> cache, MessagePublisher publisher, File archiveRepository
    ) {
      counterCache = cache;
      this.publisher = publisher;
      this.archiveRepository = archiveRepository;
    }

    @Override
    public void handleMessage(DwcaMetasyncFinishedMessage message) {
      UUID uuid = message.getDatasetUuid();

      Archive archive;
      try {
        archive = LenientArchiveFactory.openArchive(new File(archiveRepository, uuid.toString()));
      } catch (IOException e) {
        LOG.error("Could not open archive for dataset [{}]", uuid, e);
        DistributedAtomicLong distributedAtomicLong = counterCache.getUnchecked(CrawlerNodePaths.getCrawlInfoPath(
          message.getDatasetUuid(),
          CrawlerNodePaths.PAGES_FRAGMENTED_ERROR));
        try {
          distributedAtomicLong.trySet(1L);
        } catch (Exception e1) {
          LOG.warn("Failed to update counter for failed DwC-A fragment", e1);
        }
        return;
      }

      if (message.getDatasetType() == DatasetType.OCCURRENCE) {
        handleOccurrenceCore(uuid, archive, message);

      } else if (archive.getExtension(DwcTerm.Occurrence) != null) {
        handleOccurrenceExtension(uuid, archive, DwcTerm.Occurrence, message);

      } else if (archive.getExtension(GbifTerm.TypesAndSpecimen) != null) {
        handleOccurrenceExtension(uuid, archive, GbifTerm.TypesAndSpecimen, message);

      } else {
        LOG.info("Ignoring DwC-A for dataset [{}] because it does not have Occurrence information.",
                 message.getDatasetUuid());
      }

      setPagesFragmentedSuccessful(message);
    }

    private void handleOccurrenceCore(UUID uuid, Archive archive, DwcaMetasyncFinishedMessage message) {
      LOG.info("Fragmenting DwC-A for dataset [{}] with occurrence core", uuid);

      ClosableIterator<StarRecord> iterator = archive.iteratorRaw();
      int counter = 0;
      while (iterator.hasNext()) {
        StarRecord record = iterator.next();
        counter++;

        byte[] serializedRecord = StarRecordSerializer.toJson(record);
        try {
          publisher.send(new OccurrenceFragmentedMessage(uuid,
                                                         message.getAttempt(),
                                                         serializedRecord,
                                                         OccurrenceSchemaType.DWCA,
                                                         EndpointType.DWC_ARCHIVE,
                                                         message.getValidationReport()));
        } catch (IOException e) {
          LOG.error("Could not send message for dataset [{}]", uuid, e);
        }
        if (counter % COUNTER_UPDATE_FREQUENCY == 0) {
          incrementCounters(message, COUNTER_UPDATE_FREQUENCY);
        }
      }

      incrementCounters(message, counter % COUNTER_UPDATE_FREQUENCY);
      LOG.info("Successfully extracted [{}] records out of DwC-A for dataset [{}]", counter, uuid);
    }

    /**
     * Uses a nested iterator over the star and the occurrence extension to produce unique, falttened occurrence
     * fragments.
     */
    private void handleOccurrenceExtension(UUID uuid, Archive archive, Term rowType,
                                           DwcaMetasyncFinishedMessage message) {
      LOG.info("Fragmenting DwC-A for dataset [{}] with {} extension", uuid, rowType);

      ClosableIterator<StarRecord> iterator = archive.iteratorRaw();
      try {
        int counter = 0;
        while (iterator.hasNext()) {
          StarRecord record = iterator.next();

          List<Record> records = record.extension(rowType);
          if (records != null) {
            for (Record rec : record.extension(rowType)) {
              counter++;
              byte[] serializedRecord = StarRecordSerializer.toJson(record.core(), rec);
              try {
                publisher.send(new OccurrenceFragmentedMessage(uuid,
                                                               message.getAttempt(),
                                                               serializedRecord,
                                                               OccurrenceSchemaType.DWCA,
                                                               EndpointType.DWC_ARCHIVE,
                                                               message.getValidationReport()));
              } catch (IOException e) {
                LOG.error("Could not send message for dataset [{}]", uuid, e);
              }
              if (counter % COUNTER_UPDATE_FREQUENCY == 0) {
                incrementCounters(message, COUNTER_UPDATE_FREQUENCY);
              }
            }
          }
        }
        incrementCounters(message, counter % COUNTER_UPDATE_FREQUENCY);
        LOG.info("Successfully extracted [{}] records out of DwC-A for dataset [{}]", counter, uuid);

      } finally {
        iterator.close();
      }

    }

    private void setPagesFragmentedSuccessful(DwcaMetasyncFinishedMessage message) {
      DistributedAtomicLong dal =
        counterCache.getUnchecked(message.getDatasetUuid() + "/" + CrawlerNodePaths.PAGES_FRAGMENTED_SUCCESSFUL);
      try {
        dal.trySet(1L);
      } catch (Exception e) {
        LOG.error("Failed to update counter for successful DwC-A fragmenting", e);
      }
    }

    private void incrementCounters(DwcaMetasyncFinishedMessage message, long count) {
      DistributedAtomicLong fragmentsEmittedCounter =
        counterCache.getUnchecked(message.getDatasetUuid() + "/" + CrawlerNodePaths.FRAGMENTS_EMITTED);
      try {
        if (count > 0) {
          handleCounterResponse(message, fragmentsEmittedCounter.add(count));
        }
      } catch (Exception e) {
        LOG.warn("Failed to update counter of pages crawled for [{}]", message.getDatasetUuid(), e);
      }
    }

    private void handleCounterResponse(DatasetBasedMessage message, AtomicValue<Long> value) {
      if (value.succeeded()) {
        LOG.debug("Set counter of pages crawled for [{}] to [{}]", message.getDatasetUuid(), value.postValue());
      } else {
        LOG.warn("Failed to update counter of pages crawled for [{}]", message.getDatasetUuid(), value.postValue());
      }
    }

  }
}
