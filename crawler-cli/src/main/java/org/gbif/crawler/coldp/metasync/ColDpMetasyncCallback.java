package org.gbif.crawler.coldp.metasync;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.crawler.common.OkHttpRegistryMetadataClient;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.crawler.coldp.ColDpConfiguration;

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

public class ColDpMetasyncCallback extends AbstractMessageCallback<ColDpDownloadFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(ColDpMetasyncCallback.class);

  private final OkHttpRegistryMetadataClient registryClient;
  private final File archiveRepository;
  private final CuratorFramework curator;
  private final ColDpMetadataDocumentConverter converter;

  public ColDpMetasyncCallback(
      OkHttpRegistryMetadataClient registryClient,
      File archiveRepository,
      CuratorFramework curator,
      ColDpMetadataDocumentConverter converter) {
    this.registryClient = registryClient;
    this.archiveRepository = archiveRepository;
    this.curator = curator;
    this.converter = converter;
  }

  @Override
  public void handleMessage(ColDpDownloadFinishedMessage message) {
    UUID datasetKey = message.getDatasetUuid();
    try (MDC.MDCCloseable ignored1 = MDC.putCloseable("datasetKey", datasetKey.toString());
        MDC.MDCCloseable ignored2 =
            MDC.putCloseable("attempt", String.valueOf(message.getAttempt()))) {
      try {
        File archive = resolveArchive(datasetKey, message.getAttempt());
        ColDpMetadataExtractionResult result = converter.extractDocuments(archive);

        ColDpMetadataDocument fmt = result.getFormatDocument();

        if (result.hasEml()) {
          ColDpMetadataDocument eml = result.getEmlDocument();
          try (var doc = eml.rawDocumentStream()) {
            registryClient.insertMetadata(
                datasetKey, doc.readAllBytes(), fmt.getContentJson(), eml.getMetadataType());
          }
          LOG.info(
              "Forwarded EML metadata to registry for dataset [{}]",
              datasetKey);
        } else {
          registryClient.insertMetadata(datasetKey, fmt.getContentJson().getBytes(java.nio.charset.StandardCharsets.UTF_8), fmt.getContentJson(), fmt.getMetadataType());
          LOG.info(
              "Forwarded COLDP format document metadata to registry for dataset [{}]", datasetKey);
        }

        markFinished(datasetKey);
      } catch (Exception e) {
        LOG.error("Exception caught during COLDP metadata sync [{}]", datasetKey, e);
        createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
        markFinished(datasetKey);
      }
    }
  }

  private File resolveArchive(UUID datasetKey, int attempt) {
    File datasetDirectory = new File(archiveRepository, datasetKey.toString());
    File attemptArchive =
        new File(
            datasetDirectory,
            datasetKey + "." + attempt + ColDpConfiguration.COLDP_SUFFIX);
    if (attemptArchive.exists()) {
      return attemptArchive;
    }

    File latestArchive = new File(datasetDirectory, datasetKey + ColDpConfiguration.COLDP_SUFFIX);
    if (latestArchive.exists()) {
      return latestArchive;
    }
    throw new IllegalArgumentException("No COLDP archive found for dataset " + datasetKey);
  }

  private void markFinished(UUID datasetKey) {
    createOrUpdate(curator, datasetKey, PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_CHECKLIST, ProcessState.FINISHED);
    createOrUpdate(curator, datasetKey, PROCESS_STATE_SAMPLE, ProcessState.FINISHED);
  }
}
