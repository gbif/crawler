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
import org.gbif.utils.file.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedList;
import java.util.function.Consumer;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.MDC;

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
      log.warn("Mark datasetKey {} as FINISHED and finish reason is ABORT. {}", message.getDatasetUuid(), ex.getMessage());
      createOrUpdate(curator, message.getDatasetUuid(), FINISHED_REASON, FinishReason.ABORT);
      createOrUpdate(curator, message.getDatasetUuid(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    }
  }

  /** Path/File to the compressed file.*/
  private Path compressedFilePath(String datasetKey, Integer attempt) {
    return Paths.get(config.archiveRepository.toString(), datasetKey, datasetKey + "." + attempt + ".camtrapdp");
  }

  /** Path/Directory where the file is decompressed.*/
  private Path unpackedDpPath(String datasetKey) {
    return Paths.get(config.unpackedDpRepository.toString(), datasetKey);
  }

  /** Path where the DwC-A ois generated.*/
  private Path unpackedDwcaRepository(String datasetKey) {
    return Paths.get(config.unpackedDwcaRepository.toString(), datasetKey);
  }

  /** Unpacks the CamtrapDP into the target directory.*/
  @SneakyThrows
  private void decompress(CamtrapDpDownloadFinishedMessage message) {
    log.info("Decompress archive for datasetKey {}", message.getDatasetUuid());
    String datasetKey = message.getDatasetUuid().toString();
    File compressedFile = compressedFilePath(datasetKey, message.getAttempt()).toFile();
    File target = unpackedDpPath(datasetKey).toFile();
    CompressionUtil.decompressFile(target, compressedFile, true);
  }

  /** Deletes a directory recursively if it exists.*/
  private static void deleteDirectoryIfExist(File directory) {
    if (directory.exists()) {
      FileUtils.deleteDirectoryRecursively(directory);
    }
  }

  /** Line-by-line consumes a InputStream.*/
  @SneakyThrows
  private void streamInputStream(InputStream inputStream, Consumer<String> consumer) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      while (reader.ready()) {
        consumer.accept(reader.readLine());
      }
    }
  }

  /** Execute the Camtraptor from the Docker Container.*/
  private void executeCamtraptorToDwc(Path dpInput, Path dwcaOutput, Dataset dataset) {
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

      streamInputStream(process.getInputStream(), log::info);
      streamInputStream(process.getErrorStream(), log::warn);

      if (exitValue != 0) {
        log.error("Failed to convert CamtrapDP {} to DWCA, exit status {}", dataset.getKey(), exitValue);
        throw new RuntimeException("Camtraptor failed with status " + exitValue);
      }
    } catch (IOException | InterruptedException e) {
      log.error("Failed to convert CamtrapDP {} to DWCA", dataset.getKey(), e);
      throw new RuntimeException(e);
    }
  }

  /** Calls the Camtraptor Service to transform the data package into DwC-A. */
  private void toDwca(CamtrapDpDownloadFinishedMessage message) {
    try (MDC.MDCCloseable ignored1 = MDC.putCloseable("datasetKey", message.getDatasetUuid().toString());
         MDC.MDCCloseable ignored2 = MDC.putCloseable("attempt", String.valueOf(message.getAttempt()))) {

      decompress(message);

      Dataset dataset = datasetClient.get(message.getDatasetUuid());
      String datasetKey = dataset.getKey().toString();

      Path dpInput = unpackedDpPath(datasetKey);
      Path dwcaOutput = unpackedDwcaRepository(datasetKey);

      deleteDirectoryIfExist(dwcaOutput.toFile());

      executeCamtraptorToDwc(dpInput, dwcaOutput, dataset);
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
