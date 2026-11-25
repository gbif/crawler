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
package org.gbif.crawler.coldp.downloader;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.MetadataType;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.crawler.common.EndpointEml;
import org.gbif.crawler.common.HostRestrictions;
import org.gbif.crawler.common.OkHttpRegistryMetadataClient;
import org.gbif.crawler.common.SizeLimits;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import static org.gbif.crawler.CrawlerCoordinatorServiceImpl.EML_URL_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class ColDpCrawlConsumerTest {

  @TempDir java.nio.file.Path tempDir;

  @Test
  void createsColdDpFinishedMessage() {
    ColDpCrawlConsumer consumer =
        new ColDpCrawlConsumer(
            null, null, tempDir.toFile(), 10_000, new SizeLimits(), new HostRestrictions());

    CrawlJob crawlJob =
        new CrawlJob(
            UUID.randomUUID(), 3, EndpointType.COLDP, URI.create("https://example.org/archive.zip"));

    DatasetBasedMessage message = consumer.createFinishedMessage(crawlJob);
    UUID datasetKey = UUID.fromString("00000000-0000-0000-0000-000000000001");

    assertEquals(ColDpDownloadFinishedMessage.class, message.getClass());
    assertEquals(".coldp", consumer.getSuffix());
    assertEquals(
        new File(tempDir.toFile(), datasetKey.toString()).getAbsolutePath(),
        consumer.getArchiveDirectory(tempDir.toFile(), datasetKey).getAbsolutePath());
  }

  @Test
  void syncsEndpointEmlWithYamlWhenArchiveNotModified() throws Exception {
    OkHttpRegistryMetadataClient registryClient = mock(OkHttpRegistryMetadataClient.class);
    ColDpCrawlConsumer consumer =
        new ColDpCrawlConsumer(
            null,
            null,
            tempDir.toFile(),
            10_000,
            new SizeLimits(),
            new HostRestrictions(),
            registryClient);
    UUID datasetKey = UUID.randomUUID();
    File datasetDirectory = consumer.getArchiveDirectory(tempDir.toFile(), datasetKey);
    File archive = createArchive(datasetDirectory, datasetKey);
    Files.writeString(
        EndpointEml.file(datasetDirectory, datasetKey, 4).toPath(),
        "<eml:eml><dataset><title>Endpoint</title></dataset></eml:eml>");

    consumer.afterNotModified(datasetKey, crawlJob(datasetKey), datasetDirectory, archive);

    ArgumentCaptor<String> contentJson = ArgumentCaptor.forClass(String.class);
    verify(registryClient)
        .insertMetadata(eq(datasetKey), any(), contentJson.capture(), eq(MetadataType.EML));
    verifyNoMoreInteractions(registryClient);
    assertTrue(contentJson.getValue().contains("\"title\":\"Sample Checklist\""));
  }

  @Test
  void syncsYamlWhenArchiveNotModifiedAndMoreRecentThanEml() throws Exception {
    OkHttpRegistryMetadataClient registryClient = mock(OkHttpRegistryMetadataClient.class);
    ColDpCrawlConsumer consumer =
        new ColDpCrawlConsumer(
            null,
            null,
            tempDir.toFile(),
            10_000,
            new SizeLimits(),
            new HostRestrictions(),
            registryClient);
    UUID datasetKey = UUID.randomUUID();
    File datasetDirectory = consumer.getArchiveDirectory(tempDir.toFile(), datasetKey);
    // no endpoint EML file: the downloader dropped it as the archive was more recent
    File archive = createArchive(datasetDirectory, datasetKey);

    consumer.afterNotModified(datasetKey, crawlJob(datasetKey), datasetDirectory, archive);

    ArgumentCaptor<String> contentJson = ArgumentCaptor.forClass(String.class);
    verify(registryClient)
        .insertMetadata(eq(datasetKey), any(), contentJson.capture(), eq(MetadataType.COL_DP));
    verifyNoMoreInteractions(registryClient);
    assertTrue(contentJson.getValue().contains("\"title\":\"Sample Checklist\""));
  }

  private static File createArchive(File datasetDirectory, UUID datasetKey) throws Exception {
    datasetDirectory.mkdirs();
    File archive = new File(datasetDirectory, datasetKey + ".coldp");
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archive))) {
      zos.putNextEntry(new ZipEntry("metadata.yaml"));
      zos.write("title: Sample Checklist\n".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    return archive;
  }

  private static CrawlJob crawlJob(UUID datasetKey) {
    return new CrawlJob(
        datasetKey,
        EndpointType.COLDP,
        URI.create("https://example.org/a.zip"),
        4,
        Map.of(EML_URL_PROPERTY, "https://example.org/eml.xml"));
  }
}
