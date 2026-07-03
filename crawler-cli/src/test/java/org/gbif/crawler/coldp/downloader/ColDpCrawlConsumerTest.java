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
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ColDpCrawlConsumerTest {

  @TempDir java.nio.file.Path tempDir;

  @Test
  void createsColdDpFinishedMessage() {
    ColDpCrawlConsumer consumer =
        new ColDpCrawlConsumer(null, null, tempDir.toFile(), 10_000);

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
}
