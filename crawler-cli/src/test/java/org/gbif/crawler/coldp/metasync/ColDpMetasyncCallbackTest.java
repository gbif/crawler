package org.gbif.crawler.coldp.metasync;

import org.gbif.api.model.registry.Contact;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.crawler.coldp.ColDpConfiguration;
import org.gbif.crawler.coldp.metadata.ColDpMetadataParser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;
import static org.gbif.crawler.constants.CrawlerNodePaths.getCrawlInfoPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  void updatesDatasetFromDownloadedArchiveAndMarksCrawlFinished() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    createArchive(datasetKey, 2);

    Dataset dataset = new Dataset();
    dataset.setKey(datasetKey);
    dataset.setIdentifiers(new ArrayList<Identifier>());
    Contact existingContact = new Contact();
    existingContact.setKey(11);
    dataset.setContacts(List.of(existingContact));
    when(datasetService.get(datasetKey)).thenReturn(dataset);

    ColDpMetasyncCallback callback =
        new ColDpMetasyncCallback(
            datasetService,
            tempDir.toFile(),
            curator,
            new ColDpMetadataParser(),
            new ColDpMetadataSynchronizer(datasetService));

    callback.handleMessage(
        new ColDpDownloadFinishedMessage(
            datasetKey,
            URI.create("https://example.org/archive.zip"),
            2,
            new Date(),
            true,
            org.gbif.api.vocabulary.EndpointType.COLDP,
            org.gbif.common.messaging.api.messages.Platform.ALL));

    ArgumentCaptor<Dataset> datasetCaptor = ArgumentCaptor.forClass(Dataset.class);
    verify(datasetService).update(datasetCaptor.capture());
    Dataset updated = datasetCaptor.getValue();
    assertEquals("Sample Checklist", updated.getTitle());
    assertEquals("Example description\n\nAdditional notes", updated.getDescription());
    assertEquals(URI.create("https://example.org/checklist"), updated.getHomepage());
    assertEquals(URI.create("https://example.org/logo.png"), updated.getLogoUrl());
    assertEquals("10.15468/2zjeva", updated.getDoi().getDoiName());

    verify(datasetService).deleteContact(datasetKey, 11);
    verify(datasetService, times(4)).addContact(eq(datasetKey), any(Contact.class));
    verify(datasetService, times(2)).addIdentifier(eq(datasetKey), any(Identifier.class));

    ArgumentCaptor<InputStream> metadataCaptor = ArgumentCaptor.forClass(InputStream.class);
    verify(datasetService).insertMetadata(eq(datasetKey), metadataCaptor.capture());
    String eml = new String(metadataCaptor.getValue().readAllBytes(), StandardCharsets.UTF_8);
    assertTrue(eml.contains("<title xml:lang=\"eng\">Sample Checklist</title>"));
    assertTrue(eml.contains("<alternateIdentifier>doi:10.15468/2zjeva</alternateIdentifier>"));
    assertTrue(eml.contains("<electronicMailAddress>jane@example.org</electronicMailAddress>"));

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

  private File createArchive(UUID datasetKey, int attempt) throws Exception {
    File datasetDirectory = tempDir.resolve(datasetKey.toString()).toFile();
    datasetDirectory.mkdirs();
    File archive = new File(datasetDirectory, datasetKey + "." + attempt + ColDpConfiguration.COLDP_SUFFIX);

    try (ZipOutputStream outputStream = new ZipOutputStream(new FileOutputStream(archive))) {
      outputStream.putNextEntry(new ZipEntry("metadata.yaml"));
      outputStream.write(
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
      outputStream.closeEntry();
    }

    return archive;
  }
}
