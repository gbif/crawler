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
import org.gbif.api.model.registry.Contact;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.ContactType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.License;
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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

  private static final Map<String, ContactType> ROLE_MAPPING;

  static {
    // Initialize the role mapping once
    ROLE_MAPPING = new HashMap<>();
    ROLE_MAPPING.put("principalInvestigator", ContactType.PRINCIPAL_INVESTIGATOR);
    ROLE_MAPPING.put("rightsHolder", ContactType.OWNER);
    ROLE_MAPPING.put("publisher", ContactType.DISTRIBUTOR);
    ROLE_MAPPING.put("contributor", ContactType.CONTENT_PROVIDER);
    ROLE_MAPPING.put("contact", ContactType.POINT_OF_CONTACT);
  }

  protected static final ObjectMapper MAPPER = new ObjectMapper();

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
      log.warn(
          "Mark datasetKey {} as FINISHED and finish reason is ABORT. {}",
          message.getDatasetUuid(),
          ex.getMessage());
      createOrUpdate(curator, message.getDatasetUuid(), FINISHED_REASON, FinishReason.ABORT);
      createOrUpdate(
          curator, message.getDatasetUuid(), PROCESS_STATE_OCCURRENCE, ProcessState.FINISHED);
    }
  }

  /** Path/File to the compressed file. */
  private Path compressedFilePath(String datasetKey, Integer attempt) {
    return Paths.get(
        config.archiveRepository.toString(), datasetKey, datasetKey + "." + attempt + ".camtrapdp");
  }

  /** Path/Directory where the file is decompressed. */
  private Path unpackedDpPath(String datasetKey) {
    return Paths.get(config.unpackedDpRepository.toString(), datasetKey);
  }

  /** Path where the DwC-A ois generated. */
  private Path unpackedDwcaRepository(String datasetKey) {
    return Paths.get(config.unpackedDwcaRepository.toString(), datasetKey);
  }

  /** Unpacks the CamtrapDP into the target directory. */
  @SneakyThrows
  private void decompress(CamtrapDpDownloadFinishedMessage message) {
    log.info("Decompressing archive for datasetKey {} ...", message.getDatasetUuid());
    String datasetKey = message.getDatasetUuid().toString();
    File compressedFile = compressedFilePath(datasetKey, message.getAttempt()).toFile();
    File target = unpackedDpPath(datasetKey).toFile();
    CompressionUtil.decompressFile(target, compressedFile, true);
    log.info("Decompressing is finihed, datasetKey {} ...", message.getDatasetUuid());
  }

  /** Deletes a directory recursively if it exists. */
  private static void recreateDirectory(File directory) {
    if (directory.exists()) {
      FileUtils.deleteDirectoryRecursively(directory);
    }
    directory.mkdirs();
  }

  /** Line-by-line consumes a InputStream. */
  @SneakyThrows
  private void streamInputStream(InputStream inputStream, Consumer<String> consumer) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      while (reader.ready()) {
        consumer.accept(reader.readLine());
      }
    }
  }

  /** Execute the Camtraptor from the Docker Container. */
  private void executeCamtraptorToDwc(Path dpInput, Path dwcaOutput, Dataset dataset) {
    log.info("Executing camtraptor-to-dwca image...");
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
        log.error(
            "Failed to convert CamtrapDP {} to DWCA, exit status {}", dataset.getKey(), exitValue);
        throw new RuntimeException("Camtraptor failed with status " + exitValue);
      }
    } catch (IOException | InterruptedException e) {
      log.error("Failed to convert CamtrapDP {} to DWCA", dataset.getKey(), e);
      throw new RuntimeException(e);
    }
  }

  /** Calls the Camtraptor Service to transform the data package into DwC-A. */
  private void toDwca(CamtrapDpDownloadFinishedMessage message) {
    try (MDC.MDCCloseable ignored1 =
            MDC.putCloseable("datasetKey", message.getDatasetUuid().toString());
        MDC.MDCCloseable ignored2 =
            MDC.putCloseable("attempt", String.valueOf(message.getAttempt()))) {

      decompress(message);

      Dataset dataset = datasetClient.get(message.getDatasetUuid());
      String datasetKey = dataset.getKey().toString();

      Path dpInput = unpackedDpPath(datasetKey);
      Path dwcaOutput = unpackedDwcaRepository(datasetKey);

      updateLicense(dpInput, dataset);
      updateContacts(dpInput, dataset);

      recreateDirectory(dwcaOutput.toFile());

      executeCamtraptorToDwc(dpInput, dwcaOutput, dataset);
    }
  }

  private void updateLicense(Path dpInput, Dataset dataset) {
    // read the license and update it in the dataset
    Optional<License> licenseOptional = readLicense(dpInput);
    if (licenseOptional.isPresent()) {
      log.info("License {} found for camtrapDP dataset {}", licenseOptional.get().name(), dataset.getKey());
      dataset.setLicense(licenseOptional.get());
      datasetClient.update(dataset);
    } else {
      log.error("License not found for camtrapDP dataset {}", dataset.getKey());
      throw new IllegalArgumentException("License not found for camtrapDP dataset " + dataset.getKey());
    }
  }

  @SneakyThrows
  private static Optional<License> readLicense(Path unpackedDpPath) {
    File dataPackage = Paths.get(unpackedDpPath.toString(), "datapackage.json").toFile();

    JsonNode root = MAPPER.readTree(dataPackage);
    for (JsonNode licenseNode : root.path("licenses")) {
      if ("data".equals(licenseNode.path("scope").asText(null))) {
        return License.fromString(licenseNode.path("name").asText(null));
      }
    }

    return Optional.empty();
  }

  private void updateContacts(Path dpInput, Dataset dataset) {
    // read the contributors
    List<Contact> contributors = readContributors(dpInput);

    // replace contacts
    for (Contact contact : dataset.getContacts()) {
      datasetClient.deleteContact(dataset.getKey(), contact.getKey());
    }

    for (Contact contributor : contributors) {
      datasetClient.addContact(dataset.getKey(), contributor);
    }
  }

  @SneakyThrows
  private static List<Contact> readContributors(Path unpackedDpPath) {
    List<Contact> contacts = new ArrayList<>();

    File dataPackage = Paths.get(unpackedDpPath.toString(), "datapackage.json").toFile();
    JsonNode root = MAPPER.readTree(dataPackage);

    for (JsonNode contributorNode : root.path("contributors")) {
      Contact contact = new Contact();

      setContactFieldFromJsonNode(
          contributorNode, "firstName", node -> contact.setFirstName(node.asText()));
      setContactFieldFromJsonNode(
          contributorNode, "lastName", node -> contact.setLastName(node.asText()));
      setContactFieldFromJsonNode(
          contributorNode, "path", node -> setUserIdOrHomepage(node, contact));
      setContactFieldFromJsonNode(
          contributorNode, "email", node -> contact.addEmail(node.asText()));
      setContactFieldFromJsonNode(
          contributorNode, "role", node -> contact.setType(mapRoleToContactType(node.asText())));
      setContactFieldFromJsonNode(
          contributorNode, "title", node -> contact.setOrganization(node.asText()));
      setContactFieldFromJsonNode(
          contributorNode, "organization", node -> contact.setOrganization(node.asText()));

      contacts.add(contact);
    }

    return contacts;
  }

  // Sets "orcid.org" URLs as a user id, otherwise as a homepage
  private static void setUserIdOrHomepage(JsonNode pathNode, Contact contact) {
    String path = pathNode.asText();

    if (path.contains("orcid.org")) {
      contact.addUserId(path);
    } else {
      contact.addHomepage(URI.create(path));
    }
  }

  private static void setContactFieldFromJsonNode(
      JsonNode contributorNode, String fieldName, Consumer<JsonNode> valueConsumer) {
    JsonNode fieldNode = contributorNode.get(fieldName);
    if (fieldNode != null) {
      valueConsumer.accept(fieldNode);
    }
  }

  private static ContactType mapRoleToContactType(String camtrapContributorRole) {
    return ROLE_MAPPING.getOrDefault(camtrapContributorRole, ContactType.POINT_OF_CONTACT);
  }

  /** Creates and sends a message to the next step: DwC-A validation. */
  @SneakyThrows
  private void notifyNextStep(CamtrapDpDownloadFinishedMessage message) {
    log.info("Notify next step");
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
