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
package org.gbif.crawler.dwca.metasync;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaMetasyncFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.dwca.DwcaConfiguration;
import org.gbif.crawler.dwca.DwcaService;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.utils.file.ClosableIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;

/**
 * Service that listens to DwcaValidationFinishedMessages and puts found metadata documents into the
 * metadata repository thereby updating the registered datasets information. Only the metadata of
 * the dataset itself is written. Constituent metadata files within an archive (e.g. the dataset
 * folder of Catalogue of Life archives) are ignored and never registered as datasets, see
 * https://github.com/gbif/crawler/issues/97
 */
public class DwcaMetasyncService extends DwcaService {

  private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

  private static final Logger LOG = LoggerFactory.getLogger(DwcaMetasyncService.class);
  private final DwcaMetasyncConfiguration configuration;

  public DwcaMetasyncService(DwcaMetasyncConfiguration configuration) {
    super(configuration);
    this.configuration = configuration;
  }

  @Override
  protected void bindListeners() throws IOException {
    CuratorFramework curator = configuration.zooKeeper.getCuratorFramework();

    // listen to DwcaValidationFinishedMessage messages
    listener.listen(
        "dwca-metasync",
        config.poolSize,
        new DwcaValidationFinishedMessageCallback(
            datasetService, config.unpackedRepository, publisher, curator));
  }

  private static class DwcaValidationFinishedMessageCallback
      extends AbstractMessageCallback<DwcaValidationFinishedMessage> {

    private final DatasetService datasetService;
    private final File unpackDirectory;
    private final MessagePublisher publisher;
    private final CuratorFramework curator;

    private final Counter messageCount =
      METRIC_REGISTRY.counter(MetricRegistry.name(DwcaMetasyncService.class, "messageCount"));
    private final Counter datasetsUpdated =
      METRIC_REGISTRY.counter(MetricRegistry.name(DwcaMetasyncService.class, "datasetsUpdated"));

    private DwcaValidationFinishedMessageCallback(
        DatasetService datasetService,
        File unpackDirectory,
        MessagePublisher publisher,
        CuratorFramework curator) {
      this.unpackDirectory = unpackDirectory;
      this.datasetService = datasetService;
      this.publisher = publisher;
      this.curator = curator;
    }

    @Override
    public void handleMessage(DwcaValidationFinishedMessage message) {
      messageCount.inc();
      UUID uuid = message.getDatasetUuid();
      try (MDC.MDCCloseable ignored1 = MDC.putCloseable("datasetKey", uuid.toString());
          MDC.MDCCloseable ignored2 =
              MDC.putCloseable("attempt", String.valueOf(message.getAttempt()))) {
        // Sub-try so the MDC is still present for the exception logging.
        try {
          LOG.info("Updating metadata from DwC-A for dataset [{}]", uuid);
          handleMessageInternal(message, uuid);
        } catch (Exception e) {
          LOG.error("Exception caught during metasyncing DwC-A [{}]", uuid, e);
          updateZookeeper(uuid);
        }
      }
    }

    /**
     * Converts an Archive to a Dataset.DwcA object.
     *
     * @param archive the Archive to convert
     * @return a Dataset.DwcA object representing the archive
     */
    private static Dataset.DwcA fromArchive(Archive archive) {
      Dataset.DwcA dwca = new Dataset.DwcA();
      dwca.setCoreType(archive.getCore().getRowType().qualifiedName());
      if (archive.getExtensions() != null) {
        dwca.setExtensions(archive.getExtensions().stream()
            .filter(DwcaValidationFinishedMessageCallback::hasRecords)
            .map(ext -> ext.getRowType().qualifiedName())
            .collect(Collectors.toList()));
      }
      return dwca;
    }

    /**
     * Checks if the given archive file has any records.
     *
     * @param archive the archive file to check
     * @return true if the archive has records, false otherwise
     */
    private static boolean hasRecords(ArchiveFile archive) {
      try {
        try (ClosableIterator<Record> it = archive.iterator()) {
          return it.hasNext();
        }
      } catch (Exception e) {
        LOG.error("Failed to check for records in archive", e);
        return false;
      }
    }

    private void updateDwcaData(Dataset dataset, Archive archive) {
      Dataset.DwcA dwcA = fromArchive(archive);
      LOG.info("Updating existing dataset {} with DwC-A metadata: {}", dataset.getKey(), dwcA.getCoreType());
      if (dataset.getDwca() == null) {
        dataset.setDwca(dwcA);
        datasetService.createDwcaData(dataset.getKey(), dwcA);
      } else {
        datasetService.updateDwcaData(dataset.getKey(), dwcA);
      }

    }

    private void handleMessageInternal(DwcaValidationFinishedMessage message, UUID datasetKey)
        throws IOException {
      // https://github.com/gbif/portal-feedback/issues/2138
      // The DwcaValidation doesn't check the metadata anyway, so it's OK to continue.
      if (!message.getValidationReport().isValid()) {
        LOG.warn(
            "Invalid DwC-A for dataset [{}], attempting to process metadata anyway", datasetKey);
      }

      Dataset dataset = datasetService.get(datasetKey);
      if (dataset == null) {
        // exception, we don't know this dataset
        throw new IllegalArgumentException(
            "The requested dataset " + message.getDatasetUuid() + " is not registered");
      }

      // Metadata-only datasets are just a file, no DWCA.
      Archive archive;
      File metaFile;
      if (DatasetType.METADATA == dataset.getType()) {
        archive = null;
        metaFile =
            new File(
                new File(unpackDirectory, datasetKey.toString()), DwcaConfiguration.METADATA_FILE);
      } else {
        archive = DwcFiles.fromLocation(new File(unpackDirectory, datasetKey.toString()).toPath());
        metaFile = archive.getMetadataLocationFile();
        updateDwcaData(dataset, archive);
      }

      if (metaFile != null && metaFile.exists()) {
        // metadata found, put into repository thereby updating the dataset
        setMetaDocument(metaFile, datasetKey);
        datasetsUpdated.inc();
      }

      LOG.info("Finished updating metadata from DwC-A for dataset [{}]", datasetKey);

      if (Platform.OCCURRENCE.equivalent(message.getPlatform())) {
        if (message.getValidationReport().isValid()) {
          // send success message
          // constituent datasets are never registered, see https://github.com/gbif/crawler/issues/97
          publisher.send(
              new DwcaMetasyncFinishedMessage(
                  datasetKey,
                  dataset.getType(),
                  message.getSource(),
                  message.getAttempt(),
                  Collections.emptyMap(),
                  message.getValidationReport(),
                  message.getPlatform()),
              true);
        } else {
          LOG.warn(
              "Metadata processed, but not sending completion message because the archive is invalid.");
        }
      }
    }

    private boolean setMetaDocument(File metaDoc, UUID datasetKey) throws FileNotFoundException {
      try (InputStream stream = new FileInputStream(metaDoc)) {
        datasetService.insertMetadata(datasetKey, stream);
        LOG.info("Metadata document inserted from file {}", metaDoc);
        return true;
      } catch (IllegalArgumentException e) {
        LOG.warn(
            "Metadata document {} for dataset {} not understood",
            metaDoc.getAbsolutePath(),
            datasetKey,
            e);
      } catch (Exception e) {
        LOG.error(
            "Failed to upload metadata file {} for dataset {}",
            metaDoc.getAbsoluteFile(),
            datasetKey,
            e);
      }
      return false;
    }

    private void updateZookeeper(UUID uuid) {
      RetryPolicy retryPolicy = new RetryNTimes(5, 1000);
      String path = CrawlerNodePaths.getCrawlInfoPath(uuid, PAGES_FRAGMENTED_ERROR);
      DistributedAtomicLong dal = new DistributedAtomicLong(curator, path, retryPolicy);
      try {
        dal.trySet(1L);
      } catch (Exception e) {
        LOG.error("Failed to update counter for successful DwC-A fragmenting", e);
      }
    }
  }
}
