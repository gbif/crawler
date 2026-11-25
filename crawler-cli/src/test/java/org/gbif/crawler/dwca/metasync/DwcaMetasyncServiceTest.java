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
package org.gbif.crawler.dwca.metasync;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.dwca.DwcaConfiguration;
import org.gbif.crawler.dwca.metasync.DwcaMetasyncService.DwcaValidationFinishedMessageCallback;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DwcaMetasyncServiceTest {

  @TempDir File unpackedRepository;

  private final UUID datasetKey = UUID.randomUUID();
  private DatasetService datasetService;

  @BeforeEach
  public void setup() {
    datasetService = mock(DatasetService.class);
    Dataset dataset = new Dataset();
    dataset.setKey(datasetKey);
    dataset.setType(DatasetType.METADATA);
    when(datasetService.get(datasetKey)).thenReturn(dataset);
  }

  @Test
  public void testNothingUnpackedSkipsMetadata() throws Exception {
    callback(100).handleMessageInternal(message(), datasetKey);

    verify(datasetService, never()).insertMetadata(any(UUID.class), any(InputStream.class));
  }

  @Test
  public void testOversizedMetadataIsNotSent() throws Exception {
    writeMetadata(101);

    callback(100).handleMessageInternal(message(), datasetKey);

    verify(datasetService, never()).insertMetadata(any(UUID.class), any(InputStream.class));
  }

  @Test
  public void testMetadataWithinLimitIsSent() throws Exception {
    writeMetadata(100);

    callback(100).handleMessageInternal(message(), datasetKey);

    verify(datasetService).insertMetadata(eq(datasetKey), any(InputStream.class));
  }

  private DwcaValidationFinishedMessageCallback callback(long maxMetadataBytes) {
    return new DwcaValidationFinishedMessageCallback(
        datasetService, unpackedRepository, null, null, maxMetadataBytes);
  }

  private void writeMetadata(int size) throws Exception {
    File directory = new File(unpackedRepository, datasetKey.toString());
    directory.mkdirs();
    Files.write(new File(directory, DwcaConfiguration.METADATA_FILE).toPath(), new byte[size]);
  }

  private DwcaValidationFinishedMessage message() {
    return new DwcaValidationFinishedMessage(
        datasetKey,
        DatasetType.METADATA,
        URI.create("https://example.org/eml.xml"),
        1,
        new DwcaValidationReport(datasetKey, "too large"),
        EndpointType.EML,
        Platform.ALL);
  }
}
