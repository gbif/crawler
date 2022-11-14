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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedList;

import org.apache.curator.framework.CuratorFramework;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.slf4j.MDC;

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
      log.warn("Mark datasetKey {} as FINISHED and finish reason is ABORT. {}", message.getDatasetUuid(), ex.getMessage());
      createOrUpdate(curator, message.getDatasetUuid(), FINISHED_REASON, FinishReason.ABORT);
      createOrUpdate(curator, message.getDatasetUuid(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    }
  }

  @SneakyThrows
  private void decompress(CamtrapDpDownloadFinishedMessage message) {
    log.info("Decompress archive for datasetKey {}", message.getDatasetUuid());
    String datasetKey = message.getDatasetUuid().toString();
    Path source =
        Paths.get(
            config.archiveRepository.toString(),
            datasetKey,
            datasetKey + "." + message.getAttempt() + ".camtrapdp");
    Path target = Paths.get(config.unpackedDpRepository.toString(), datasetKey);
    CompressionUtil.decompressFile(target.toFile(), source.toFile(), true);
  }

  /** Calls the Camtraptor Service to transform the data package into DwC-A. */
  private void toDwca(CamtrapDpDownloadFinishedMessage message) {
    try (MDC.MDCCloseable ignored1 = MDC.putCloseable("datasetKey", message.getDatasetUuid().toString());
         MDC.MDCCloseable ignored2 = MDC.putCloseable("attempt", String.valueOf(message.getAttempt()))) {

      decompress(message);
      Dataset dataset = datasetClient.get(message.getDatasetUuid());

      Path dpInput = Paths.get(config.unpackedDpRepository.toString(), dataset.getKey().toString());
      Path dwcaOutput = Paths.get(config.unpackedDwcaRepository.toString(), dataset.getKey().toString());

      try {
        Integer uid = (Integer) Files.getAttribute(dpInput.toAbsolutePath(), "unix:uid");

        LinkedList<String> cmds = new LinkedList<>();
        cmds.add("/usr/bin/docker");
        cmds.add("run");
        cmds.add("--rm");
        cmds.add("--user");
        cmds.add(uid.toString());
        cmds.add("--volume");
        cmds.add(dpInput.toAbsolutePath() + ":/R/dp/:ro");
        cmds.add("--volume");
        cmds.add(dwcaOutput.toAbsolutePath() + ":/R/dwca/");
        cmds.add(config.dockerImage);
        cmds.add(dataset.getTitle());
        ProcessBuilder pb = new ProcessBuilder(cmds);

        Process process = pb.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) {
          log.error("Failed to convert CamtrapDP {} to DWCA, exit status {}", dataset.getKey(), exitValue);
        }

        try (BufferedReader stdOut = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          while (stdOut.ready()) {
            log.info(stdOut.readLine());
          }
        }

        try (BufferedReader stdErr = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
          while (stdErr.ready()) {
            log.warn(stdErr.readLine());
          }
        }

      } catch (InterruptedException e) {
        log.error("Failed to convert CamtrapDP " + dataset.getKey() + " to DWCA", e);
      } catch (IOException e) {
        log.error("Failed to convert CamtrapDP " + dataset.getKey() + " to DWCA", e);
      }
    }
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
