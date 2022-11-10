package org.gbif.crawler.camtrapdp.converter;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.CamtrapDpDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;
import org.gbif.crawler.camtrapdp.CamtrapDpConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.utils.file.CompressionUtil;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import org.apache.curator.framework.CuratorFramework;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;

/** Callback which is called when the {@link CamtrapDpDownloadFinishedMessage} is received. */
@Slf4j
@AllArgsConstructor
public class CamtrapDpToDwcaCallback
    extends AbstractMessageCallback<CamtrapDpDownloadFinishedMessage> {

  private final CamtrapDpConfiguration config;
  private final CuratorFramework curator;
  private final MessagePublisher publisher;
  private final DatasetClient datasetClient;

  @Override
  public void handleMessage(CamtrapDpDownloadFinishedMessage message) {
    try {
      toDwca(message);
      notifyNextStep(message);
    } catch (Exception ex) {
      createOrUpdate(curator, message.getDatasetUuid(), FINISHED_REASON, FinishReason.ABORT);
      createOrUpdate(curator, message.getDatasetUuid(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    }
  }

  @SneakyThrows
  private void decompress(CamtrapDpDownloadFinishedMessage message) {
    String datasetKey = message.getDatasetUuid().toString();
    Path source =
        Paths.get(
            config.archiveRepository.toString(),
            datasetKey,
            datasetKey + "." + message.getAttempt() + ".camtrapdp");
    Path target = Paths.get(config.archiveRepository.toString(), "unpacked", datasetKey);
    CompressionUtil.decompressFile(target.toFile(), source.toFile(), true);
  }

  /** Calls the Camtraptor Service to transform the data package into DwC-A. */
  private void toDwca(CamtrapDpDownloadFinishedMessage message) {
    decompress(message);
    Dataset dataset = datasetClient.get(message.getDatasetUuid());
    CamtraptorWsClient camtraptorWsClient = new CamtraptorWsClient(config.camtraptorWsUrl);
    camtraptorWsClient.toDwca(dataset.getKey(), dataset.getTitle());
  }

  /** Creates and sends a message to the next step: DwC-A validation. */
  @SneakyThrows
  private void notifyNextStep(CamtrapDpDownloadFinishedMessage message) {
    publisher.send(createOutgoingMessage(message));
  }

  /** Builds the message to be sent to the next processing stage: DwC-A validation. */
  private DwcaDownloadFinishedMessage createOutgoingMessage(
      CamtrapDpDownloadFinishedMessage message) {
    return new DwcaDownloadFinishedMessage(
        message.getDatasetUuid(),
        message.getSource(),
        message.getAttempt(),
        new Date(),
        true,
        EndpointType.DWC_ARCHIVE,
        message.getPlatform());
  }

  public String getRouting() {
    return CamtrapDpDownloadFinishedMessage.ROUTING_KEY;
  }
}
