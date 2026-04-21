package org.gbif.crawler.coldp.metasync;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.MetadataType;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.crawler.coldp.ColDpConfiguration;

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
class ColDpMetasyncCallbackTest {

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
  void forwardsYamlMetadataToRegistryAndMarksCrawlFinished() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    createArchiveWithYamlOnly(datasetKey, 2);

    ColDpMetasyncCallback callback =
        new ColDpMetasyncCallback(
            datasetService, tempDir.toFile(), curator, new ColDpMetadataDocumentConverter());

    callback.handleMessage(buildMessage(datasetKey, 2));

    ArgumentCaptor<byte[]> rawDocumentCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
    verify(datasetService)
        .insertMetadata(
            eq(datasetKey),
            rawDocumentCaptor.capture(),
            jsonCaptor.capture(),
            eq(MetadataType.COLDP));
    verifyNoMoreInteractions(datasetService);

    String rawDocument =
        new String(rawDocumentCaptor.getValue(), StandardCharsets.UTF_8);
    String json = jsonCaptor.getValue();
    assertTrue(rawDocument.contains("title: Sample Checklist"));
    assertTrue(rawDocument.contains("contact:"));
    assertTrue(json.contains("\"title\":\"Sample Checklist\""));
    assertTrue(json.contains("\"doi\":\"10.15468/2zjeva\""));
    assertTrue(json.contains("\"email\":\"jane@example.org\""));
    assertTrue(json.contains("\"organisation\":\"GBIF\""));

    assertCrawlFinished(datasetKey);
  }

  @Test
  void forwardsYamlAndEmlToRegistryWhenEmlPresent() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    createArchiveWithYamlAndEml(datasetKey, 1);

    ColDpMetasyncCallback callback =
        new ColDpMetasyncCallback(
            datasetService, tempDir.toFile(), curator, new ColDpMetadataDocumentConverter());

    callback.handleMessage(buildMessage(datasetKey, 1));

    InOrder ordered = inOrder(datasetService);

    ArgumentCaptor<byte[]> yamlStreamCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<String> yamlJsonCaptor = ArgumentCaptor.forClass(String.class);
    ordered
        .verify(datasetService)
        .insertMetadata(
            eq(datasetKey),
            yamlStreamCaptor.capture(),
            yamlJsonCaptor.capture(),
            eq(MetadataType.COLDP));

    ArgumentCaptor<byte[]> emlStreamCaptor = ArgumentCaptor.forClass(byte[].class);
    ordered
        .verify(datasetService)
        .insertMetadata(eq(datasetKey), emlStreamCaptor.capture(), isNull(), eq(MetadataType.EML));

    ordered.verifyNoMoreInteractions();

    String yamlJson = yamlJsonCaptor.getValue();
    assertTrue(yamlJson.contains("\"title\":\"Sample Checklist\""));

    String emlContent =
        new String(emlStreamCaptor.getValue(), StandardCharsets.UTF_8);
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

  private ColDpDownloadFinishedMessage buildMessage(UUID datasetKey, int attempt) {
    return new ColDpDownloadFinishedMessage(
        datasetKey,
        URI.create("https://example.org/archive.zip"),
        attempt,
        new Date(),
        true,
        org.gbif.api.vocabulary.EndpointType.COLDP,
        org.gbif.common.messaging.api.messages.Platform.ALL);
  }

  private File createArchiveWithYamlOnly(UUID datasetKey, int attempt) throws Exception {
    File archive = prepareArchiveFile(datasetKey, attempt);
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archive))) {
      writeYamlEntry(zos);
    }
    return archive;
  }

  private File createArchiveWithYamlAndEml(UUID datasetKey, int attempt) throws Exception {
    File archive = prepareArchiveFile(datasetKey, attempt);
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archive))) {
      writeYamlEntry(zos);
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
        datasetDirectory, datasetKey + "." + attempt + ColDpConfiguration.COLDP_SUFFIX);
  }

  private void writeYamlEntry(ZipOutputStream zos) throws Exception {
    zos.putNextEntry(new ZipEntry("metadata.yaml"));
    zos.write(
        """
        title: Sample Checklist
        description: Example description
        doi: 10.15468/2zjeva
        issued: 2026-01-13
        version: v1
        license: CC0-1.0
        url: https://example.org/checklist
        logo: https://example.org/logo.png
        identifier:
          col: 1010
          gbif: abc-123
        contact:
          given: Jane
          family: Doe
          email: jane@example.org
        creator:
          given: Alice
          family: Author
        editor:
          given: Ed
          family: Itor
        publisher:
          organisation: GBIF
        notes: Additional notes
        """
            .getBytes(StandardCharsets.UTF_8));
    zos.closeEntry();
  }
}
