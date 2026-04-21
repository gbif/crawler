package org.gbif.crawler.dwcdp.metasync;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.MetadataType;
import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;
import org.gbif.crawler.dwcdp.DwcDpConfiguration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;
import static org.gbif.crawler.constants.CrawlerNodePaths.getCrawlInfoPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class DwcDpMetasyncCallbackTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir java.nio.file.Path tempDir;

  @Mock private DatasetService datasetService;

  private TestingServer server;
  private CuratorFramework curator;

  @BeforeEach
  void setUp() throws Exception {
    server = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .namespace("crawler")
            .retryPolicy(new RetryOneTime(1))
            .build();
    curator.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    curator.close();
    server.close();
  }

  @Test
  void forwardsDatapackageMetadataToRegistryAndMarksCrawlFinished() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    createArchiveWithDatapackageOnly(datasetKey, 2);

    DwcDpMetasyncCallback callback =
        new DwcDpMetasyncCallback(
            datasetService, tempDir.toFile(), curator, new DwcDpMetadataDocumentConverter());

    callback.handleMessage(buildMessage(datasetKey, 2));

    ArgumentCaptor<InputStream> rawDocumentCaptor = ArgumentCaptor.forClass(InputStream.class);
    ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
    verify(datasetService)
        .insertMetadata(
            eq(datasetKey), rawDocumentCaptor.capture().readAllBytes(), jsonCaptor.capture(), eq(MetadataType.DWC_DP));
    verifyNoMoreInteractions(datasetService);

    String rawDocument =
        new String(rawDocumentCaptor.getValue().readAllBytes(), StandardCharsets.UTF_8);
    String json = jsonCaptor.getValue();
    JsonNode rawTree = MAPPER.readTree(rawDocument);
    JsonNode contentTree = MAPPER.readTree(json);
    assertEquals("Sample DwcDP", rawTree.path("name").asText());
    assertEquals("CC0-1.0", rawTree.path("license").asText());
    assertEquals("Sample DwcDP", contentTree.path("name").asText());
    assertEquals("https://doi.org/10.15468/sample", contentTree.path("id").asText());
    assertEquals("jane@example.org", contentTree.path("contributors").get(0).path("email").asText());

    assertCrawlFinished(datasetKey);
  }

  @Test
  void forwardsDatapackageAndEmlToRegistryWhenEmlPresent() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    createArchiveWithDatapackageAndEml(datasetKey, 1);

    DwcDpMetasyncCallback callback =
        new DwcDpMetasyncCallback(
            datasetService, tempDir.toFile(), curator, new DwcDpMetadataDocumentConverter());

    callback.handleMessage(buildMessage(datasetKey, 1));

    InOrder ordered = inOrder(datasetService);

    ArgumentCaptor<InputStream> dpStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
    ArgumentCaptor<String> dpJsonCaptor = ArgumentCaptor.forClass(String.class);
    ordered
        .verify(datasetService)
        .insertMetadata(
            eq(datasetKey), dpStreamCaptor.capture().readAllBytes(), dpJsonCaptor.capture(), eq(MetadataType.DWC_DP));

    ArgumentCaptor<InputStream> emlStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
    ordered
        .verify(datasetService)
        .insertMetadata(
            eq(datasetKey), emlStreamCaptor.capture().readAllBytes(), isNull(), eq(MetadataType.EML));

    ordered.verifyNoMoreInteractions();

    String dpJson = dpJsonCaptor.getValue();
    assertTrue(dpJson.contains("\"name\":\"Sample DwcDP\""));

    String emlContent =
        new String(emlStreamCaptor.getValue().readAllBytes(), StandardCharsets.UTF_8);
    assertTrue(emlContent.contains("<title>Sample EML Dataset</title>"));

    assertCrawlFinished(datasetKey);
  }

  private void assertCrawlFinished(UUID datasetKey) throws Exception {
    assertEquals(
        "FINISHED",
        new String(
            curator.getData().forPath(getCrawlInfoPath(datasetKey, PROCESS_STATE_OCCURRENCE)),
            StandardCharsets.UTF_8));
    assertEquals(
        "FINISHED",
        new String(
            curator.getData().forPath(getCrawlInfoPath(datasetKey, PROCESS_STATE_CHECKLIST)),
            StandardCharsets.UTF_8));
    assertEquals(
        "FINISHED",
        new String(
            curator.getData().forPath(getCrawlInfoPath(datasetKey, PROCESS_STATE_SAMPLE)),
            StandardCharsets.UTF_8));
  }

  private DwcDpDownloadFinishedMessage buildMessage(UUID datasetKey, int attempt) {
    return new DwcDpDownloadFinishedMessage(
        datasetKey,
        URI.create("https://example.org/archive.zip"),
        attempt,
        new Date(),
        true,
        org.gbif.api.vocabulary.EndpointType.DWC_DP,
        null,
        org.gbif.common.messaging.api.messages.Platform.ALL);
  }

  private File createArchiveWithDatapackageOnly(UUID datasetKey, int attempt) throws Exception {
    File archive = prepareArchiveFile(datasetKey, attempt);
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archive))) {
      writeDatapackageEntry(zos);
    }
    return archive;
  }

  private File createArchiveWithDatapackageAndEml(UUID datasetKey, int attempt) throws Exception {
    File archive = prepareArchiveFile(datasetKey, attempt);
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archive))) {
      writeDatapackageEntry(zos);
      zos.putNextEntry(new ZipEntry("eml.xml"));
      zos.write(
          """
          <?xml version="1.0" encoding="UTF-8"?>
          <eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1">
            <dataset>
              <title>Sample EML Dataset</title>
            </dataset>
          </eml:eml>
          """
              .getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    return archive;
  }

  private File prepareArchiveFile(UUID datasetKey, int attempt) {
    File datasetDirectory = tempDir.resolve(datasetKey.toString()).toFile();
    datasetDirectory.mkdirs();
    return new File(
        datasetDirectory, datasetKey + "." + attempt + DwcDpConfiguration.DWC_DP_SUFFIX);
  }

  private void writeDatapackageEntry(ZipOutputStream zos) throws Exception {
    zos.putNextEntry(new ZipEntry("datapackage.json"));
    zos.write(
        """
        {
          "id": "https://doi.org/10.15468/sample",
          "name": "Sample DwcDP",
          "license": "CC0-1.0",
          "contributors": [
            {
              "title": "Jane Doe",
              "email": "jane@example.org",
              "role": "publisher"
            }
          ]
        }
        """
            .getBytes(StandardCharsets.UTF_8));
    zos.closeEntry();
  }
}
