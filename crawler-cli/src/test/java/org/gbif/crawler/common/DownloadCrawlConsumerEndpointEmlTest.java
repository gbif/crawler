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
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import static org.gbif.crawler.CrawlerCoordinatorServiceImpl.EML_URL_PROPERTY;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.getCrawlInfoPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers {@link DownloadCrawlConsumer#downloadEndpointEml} against a local HTTP server. */
public class DownloadCrawlConsumerEndpointEmlTest {

  private static final String EML = "<eml:eml><dataset><title>Endpoint</title></dataset></eml:eml>";
  private static final long EML_LAST_MODIFIED =
      Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
  private static final long ARCHIVE_LAST_MODIFIED =
      Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();

  private static HttpServer server;
  private static String baseUrl;

  @TempDir File archiveRepository;

  @BeforeAll
  public static void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/eml.xml", exchange -> respond(exchange, 200, EML));
    server.createContext("/missing.xml", exchange -> respond(exchange, 404, "not found"));
    server.createContext(
        "/eml-2024.xml",
        exchange -> {
          exchange.getResponseHeaders().add("Last-Modified", httpDate(EML_LAST_MODIFIED));
          respond(exchange, 200, EML);
        });
    server.createContext(
        "/archive-2025.zip",
        exchange -> {
          exchange.getResponseHeaders().add("Last-Modified", httpDate(ARCHIVE_LAST_MODIFIED));
          respond(exchange, 200, "archive");
        });
    server.createContext(
        "/unchanged.zip",
        exchange -> {
          if (exchange.getRequestHeaders().containsKey("If-Modified-Since")) {
            exchange.sendResponseHeaders(304, -1);
            exchange.close();
          } else {
            respond(exchange, 200, "archive");
          }
        });
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterAll
  public static void stopServer() {
    server.stop(0);
  }

  @Test
  public void testDownloadsEndpointEmlNextToArchive() throws Exception {
    UUID datasetKey = UUID.randomUUID();

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob(datasetKey, baseUrl + "/eml.xml"), null);

    File eml = EndpointEml.file(archiveRepository, datasetKey, 2);
    assertEquals(EML, Files.readString(eml.toPath()));
    assertEquals(eml, EndpointEml.find(archiveRepository, datasetKey, 2).orElse(null));
  }

  @Test
  public void testHttpErrorLeavesNoFile() {
    UUID datasetKey = UUID.randomUUID();

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob(datasetKey, baseUrl + "/missing.xml"), null);

    assertFalse(EndpointEml.file(archiveRepository, datasetKey, 2).exists());
    assertTrue(EndpointEml.downloadFailed(archiveRepository, datasetKey, 2));
  }

  @Test
  public void testUnreachableEndpointIsNotFatal() {
    UUID datasetKey = UUID.randomUUID();

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob(datasetKey, "http://127.0.0.1:1/eml.xml"), null);

    assertFalse(EndpointEml.file(archiveRepository, datasetKey, 2).exists());
    assertTrue(EndpointEml.downloadFailed(archiveRepository, datasetKey, 2));
  }

  @Test
  public void testNoEmlUrlDoesNothing() {
    UUID datasetKey = UUID.randomUUID();
    CrawlJob crawlJob =
      new CrawlJob(datasetKey, EndpointType.DWC_DP, URI.create(baseUrl + "/archive.zip"), 2, null);

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob, null);

    assertFalse(EndpointEml.file(archiveRepository, datasetKey, 2).exists());
  }

  @Test
  public void testUnmodifiedArchiveStillDownloadsEndpointEml() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    // an earlier crawl left the archive, so the download is conditional and the server answers 304
    Files.writeString(new File(archiveRepository, datasetKey + ".suffix").toPath(), "archive");
    CrawlJob crawlJob =
        new CrawlJob(
            datasetKey,
            EndpointType.DWC_DP,
            URI.create(baseUrl + "/unchanged.zip"),
            2,
            Map.of(EML_URL_PROPERTY, baseUrl + "/eml.xml"));

    try (TestingServer zk = new TestingServer();
        CuratorFramework curator =
            CuratorFrameworkFactory.builder()
                .connectString(zk.getConnectString())
                .namespace("crawler")
                .retryPolicy(new RetryOneTime(1))
                .build()) {
      curator.start();
      AtomicBoolean afterNotModifiedCalled = new AtomicBoolean();
      DownloadCrawlConsumer consumer =
          new TestConsumer(curator) {
            @Override
            protected void afterNotModified(
                UUID key, CrawlJob job, File datasetDirectory, File localFile) {
              // the EML must already be there when the hook runs
              afterNotModifiedCalled.set(EndpointEml.find(datasetDirectory, key, 2).isPresent());
            }
          };

      consumer.crawl(datasetKey, crawlJob);

      assertTrue(afterNotModifiedCalled.get());
      assertEquals(
          EML, Files.readString(EndpointEml.file(archiveRepository, datasetKey, 2).toPath()));
      assertEquals(
          "NOT_MODIFIED",
          new String(
              curator.getData().forPath(getCrawlInfoPath(datasetKey, FINISHED_REASON)),
              StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testEmlIsDroppedWhenArchiveIsMoreRecent() {
    UUID datasetKey = UUID.randomUUID();

    consumer()
        .downloadEndpointEml(
            archiveRepository,
            datasetKey,
            crawlJob(datasetKey, baseUrl + "/eml-2024.xml"),
            ARCHIVE_LAST_MODIFIED);

    assertFalse(EndpointEml.find(archiveRepository, datasetKey, 2).isPresent());
    // the archive's metadata is used, the download did not fail
    assertFalse(EndpointEml.downloadFailed(archiveRepository, datasetKey, 2));
  }

  @Test
  public void testEmlIsKeptWhenMoreRecentThanArchive() {
    UUID datasetKey = UUID.randomUUID();

    consumer()
        .downloadEndpointEml(
            archiveRepository,
            datasetKey,
            crawlJob(datasetKey, baseUrl + "/eml-2024.xml"),
            EML_LAST_MODIFIED - 1000);

    File eml = EndpointEml.file(archiveRepository, datasetKey, 2);
    assertTrue(eml.exists());
    assertEquals(EML_LAST_MODIFIED, eml.lastModified());
  }

  @Test
  public void testEmlWithoutLastModifiedIsKept() {
    UUID datasetKey = UUID.randomUUID();

    // /eml.xml sends no Last-Modified, so we can't tell which is more recent
    consumer()
        .downloadEndpointEml(
            archiveRepository,
            datasetKey,
            crawlJob(datasetKey, baseUrl + "/eml.xml"),
            ARCHIVE_LAST_MODIFIED);

    assertTrue(EndpointEml.find(archiveRepository, datasetKey, 2).isPresent());
  }

  @Test
  public void testDownloadedArchiveMoreRecentThanEmlUsesArchiveMetadata() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    CrawlJob crawlJob =
        new CrawlJob(
            datasetKey,
            EndpointType.DWC_DP,
            URI.create(baseUrl + "/archive-2025.zip"),
            2,
            Map.of(EML_URL_PROPERTY, baseUrl + "/eml-2024.xml"));

    try (TestingServer zk = new TestingServer();
        CuratorFramework curator = curator(zk)) {
      curator.start();

      new TestConsumer(curator) {
        @Override
        protected void success(UUID key, CrawlJob job) {
          // no message publisher in this test
        }
      }.crawl(datasetKey, crawlJob);

      assertEquals(
          ARCHIVE_LAST_MODIFIED, new File(archiveRepository, datasetKey + ".suffix").lastModified());
      assertFalse(EndpointEml.find(archiveRepository, datasetKey, 2).isPresent());
    }
  }

  @Test
  public void testLastModifiedIsUnknownForFilesWrittenDuringDownload() throws Exception {
    File file = new File(archiveRepository, "file");
    long downloadStart = System.currentTimeMillis() - 1000;
    Files.writeString(file.toPath(), "content");

    assertNull(DownloadCrawlConsumer.lastModified(file, downloadStart));

    file.setLastModified(EML_LAST_MODIFIED);
    assertEquals(EML_LAST_MODIFIED, DownloadCrawlConsumer.lastModified(file, downloadStart));
    assertNull(DownloadCrawlConsumer.lastModified(new File(archiveRepository, "none"), downloadStart));
  }

  private static CuratorFramework curator(TestingServer zk) {
    return CuratorFrameworkFactory.builder()
        .connectString(zk.getConnectString())
        .namespace("crawler")
        .retryPolicy(new RetryOneTime(1))
        .build();
  }

  private static String httpDate(long millis) {
    return DateTimeFormatter.RFC_1123_DATE_TIME.format(
        Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC));
  }

  private static CrawlJob crawlJob(UUID datasetKey, String emlUrl) {
    return new CrawlJob(
      datasetKey,
      EndpointType.DWC_DP,
      URI.create(baseUrl + "/archive.zip"),
      2,
      Map.of(EML_URL_PROPERTY, emlUrl));
  }

  private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body)
    throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private DownloadCrawlConsumer consumer() {
    return new TestConsumer(null);
  }

  private class TestConsumer extends DownloadCrawlConsumer {

    TestConsumer(CuratorFramework curator) {
      super(curator, null, archiveRepository, 1000);
    }

    @Override
    protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
      return null;
    }

    @Override
    protected String getSuffix() {
      return ".suffix";
    }

    @Override
    protected File getArchiveDirectory(File archiveRepository, UUID datasetKey) {
      return archiveRepository;
    }
  }
}
