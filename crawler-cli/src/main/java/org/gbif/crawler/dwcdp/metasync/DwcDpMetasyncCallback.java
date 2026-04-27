package org.gbif.crawler.dwcdp.metasync;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.crawler.common.OkHttpRegistryMetadataClient;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;
import org.gbif.crawler.dwcdp.DwcDpConfiguration;

import java.io.File;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;

public class DwcDpMetasyncCallback extends AbstractMessageCallback<DwcDpDownloadFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwcDpMetasyncCallback.class);

  private final OkHttpRegistryMetadataClient registryClient;
  private final File archiveRepository;
  private final CuratorFramework curator;
  private final DwcDpMetadataDocumentConverter converter;

  public DwcDpMetasyncCallback(
      OkHttpRegistryMetadataClient registryClient,
      File archiveRepository,
      CuratorFramework curator,
      DwcDpMetadataDocumentConverter converter) {
    this.registryClient = registryClient;
    this.archiveRepository = archiveRepository;
    this.curator = curator;
    this.converter = converter;
  }

  @Override
  public void handleMessage(DwcDpDownloadFinishedMessage message) {
    UUID datasetKey = message.getDatasetUuid();
    try (MDC.MDCCloseable ignored1 = MDC.putCloseable("datasetKey", datasetKey.toString());
        MDC.MDCCloseable ignored2 =
            MDC.putCloseable("attempt", String.valueOf(message.getAttempt()))) {
      try {
        File archive = resolveArchive(datasetKey, message.getAttempt());
        DwcDpMetadataExtractionResult result = converter.extractDocuments(archive);

        DwcDpMetadataDocument dp = result.getDatapackageDocument();

        if (result.hasEml()) {
          DwcDpMetadataDocument eml = result.getEmlDocument();

          try (var doc = eml.rawDocumentStream()) {
            registryClient.insertMetadata(
                datasetKey, doc.readAllBytes(), dp.getContentJson(), eml.getMetadataType());
          }
          LOG.info(
              "Forwarded EML metadata to registry for dataset [{}]",
              datasetKey);
        } else {
          try (var doc = dp.rawDocumentStream()) {
            registryClient.insertMetadata(datasetKey, doc.readAllBytes(), dp.getContentJson(), dp.getMetadataType());
          }
          LOG.info(
              "Forwarded DwcDP datapackage metadata to registry for dataset [{}]", datasetKey);
        }

        markFinished(datasetKey);
      } catch (Exception e) {
        LOG.error("Exception caught during DwcDP metadata sync [{}]", datasetKey, e);
        createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
        markFinished(datasetKey);
      }
    }
  }

  private File resolveArchive(UUID datasetKey, int attempt) {
    File datasetDirectory = new File(archiveRepository, datasetKey.toString());
    File attemptArchive =
        new File(datasetDirectory, datasetKey + "." + attempt + DwcDpConfiguration.DWC_DP_SUFFIX);
    if (attemptArchive.exists()) {
      return attemptArchive;
    }

    File latestArchive = new File(datasetDirectory, datasetKey + DwcDpConfiguration.DWC_DP_SUFFIX);
    if (latestArchive.exists()) {
      return latestArchive;
    }
    throw new IllegalArgumentException("No DwcDP archive found for dataset " + datasetKey);
  }

  private void markFinished(UUID datasetKey) {
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }
}
