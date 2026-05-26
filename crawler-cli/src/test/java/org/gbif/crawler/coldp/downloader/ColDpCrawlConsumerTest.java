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
