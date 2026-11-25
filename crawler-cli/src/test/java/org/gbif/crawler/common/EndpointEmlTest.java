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
package org.gbif.crawler.common;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.MetadataType;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.gbif.crawler.CrawlerCoordinatorServiceImpl.EML_URL_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class EndpointEmlTest {

  private static final String EML = "<eml:eml><dataset><title>Endpoint</title></dataset></eml:eml>";
  private static final int ATTEMPT = 3;

  @TempDir File datasetDirectory;

  @Mock private OkHttpRegistryMetadataClient registryClient;

  private final UUID datasetKey = UUID.randomUUID();
  private final AtomicBoolean archiveMetadataSent = new AtomicBoolean();

  @Test
  void syncsEndpointEmlForUnmodifiedArchive() throws Exception {
    writeEml();

    sync(crawlJob(true), a -> "{\"title\":\"DP\"}");

    verify(registryClient)
        .insertMetadata(
            datasetKey,
            EML.getBytes(StandardCharsets.UTF_8),
            "{\"title\":\"DP\"}",
            MetadataType.EML);
    assertFalse(archiveMetadataSent.get());
  }

  @Test
  void sendsArchiveMetadataWhenArchiveIsMoreRecentThanEml() {
    // no EML file and no failure marker: the downloader dropped the EML as the archive was newer
    AtomicBoolean contentJsonRead = new AtomicBoolean();

    sync(
        crawlJob(true),
        a -> {
          contentJsonRead.set(true);
          return "{}";
        });

    assertTrue(archiveMetadataSent.get());
    assertFalse(contentJsonRead.get());
    verifyNoInteractions(registryClient);
  }

  @Test
  void sendsArchiveMetadataWhenRegistryRejectsEml() throws Exception {
    writeEml();
    doThrow(new IOException("HTTP 400"))
        .when(registryClient)
        .insertMetadata(eq(datasetKey), any(), any(), eq(MetadataType.EML));

    sync(crawlJob(true), a -> "{}");

    assertTrue(archiveMetadataSent.get());
  }

  @Test
  void doesNothingWhenEmlDownloadFailed() throws Exception {
    Files.write(
        EndpointEml.failedMarker(datasetDirectory, datasetKey, ATTEMPT).toPath(), new byte[0]);

    sync(crawlJob(true), a -> "{}");

    assertFalse(archiveMetadataSent.get());
    verifyNoInteractions(registryClient);
  }

  @Test
  void doesNothingWithoutEmlEndpoint() {
    sync(crawlJob(false), a -> "{}");

    assertFalse(archiveMetadataSent.get());
    verifyNoInteractions(registryClient);
  }

  @Test
  void unreadableArchiveIsNotFatal() throws Exception {
    writeEml();

    sync(
        crawlJob(true),
        a -> {
          throw new IOException("broken archive");
        });

    verifyNoInteractions(registryClient);
  }

  @Test
  void noRegistryClientDoesNothing() {
    EndpointEml.syncForUnmodifiedArchive(
        null,
        datasetDirectory,
        crawlJob(true),
        archive(),
        a -> "{}",
        a -> archiveMetadataSent.set(true));

    assertFalse(archiveMetadataSent.get());
  }

  private void sync(CrawlJob crawlJob, EndpointEml.ContentJsonReader contentJsonReader) {
    EndpointEml.syncForUnmodifiedArchive(
        registryClient,
        datasetDirectory,
        crawlJob,
        archive(),
        contentJsonReader,
        a -> archiveMetadataSent.set(true));
  }

  private CrawlJob crawlJob(boolean withEmlEndpoint) {
    return new CrawlJob(
        datasetKey,
        EndpointType.DWC_DP,
        URI.create("https://example.org/archive.zip"),
        ATTEMPT,
        withEmlEndpoint ? Map.of(EML_URL_PROPERTY, "https://example.org/eml.xml") : null);
  }

  private File archive() {
    return new File(datasetDirectory, "archive.zip");
  }

  private void writeEml() throws IOException {
    Files.writeString(EndpointEml.file(datasetDirectory, datasetKey, ATTEMPT).toPath(), EML);
  }
}
