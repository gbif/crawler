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
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A download over the size limit must not replace or remove the last good archive. */
public class DownloadCrawlConsumerSizeLimitTest {

  private static final String SUFFIX = ".suffix";

  private static TestingServer zkServer;
  private static CuratorFramework curator;

  @TempDir File archiveRepository;

  private HttpServer server;

  @BeforeAll
  public static void setupZk() throws Exception {
    zkServer = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(zkServer.getConnectString())
            .namespace("crawler-size-limit-test")
            .retryPolicy(new RetryOneTime(100))
            .build();
    curator.start();
  }

  @AfterAll
  public static void tearDownZk() throws IOException {
    curator.close();
    zkServer.stop();
  }

  @BeforeEach
  public void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    // Chunked, so the limit is hit while writing rather than from Content-Length
    server.createContext(
        "/archive.zip",
        exchange -> {
          exchange.sendResponseHeaders(200, 0);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(new byte[10_000]);
          }
        });
    server.createContext(
        "/twofiles.zip",
        exchange -> {
          exchange.sendResponseHeaders(200, 0);
          try (ZipOutputStream out = new ZipOutputStream(exchange.getResponseBody())) {
            for (String name : new String[] {"meta.xml", "occurrence.txt"}) {
              out.putNextEntry(new ZipEntry(name));
              out.write("data".getBytes(StandardCharsets.UTF_8));
              out.closeEntry();
            }
          }
        });
    server.start();
  }

  @AfterEach
  public void stopServer() {
    server.stop(0);
  }

  @Test
  public void testOversizedDownloadKeepsLastGoodArchive() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    File localFile = new File(archiveRepository, datasetKey + SUFFIX);
    Files.writeString(localFile.toPath(), "good archive");
    long lastModified = localFile.lastModified() - 3_600_000;
    localFile.setLastModified(lastModified);

    SizeLimits limits = new SizeLimits();
    limits.maxDownloadBytes = 1_000;
    HostRestrictions hostRestrictions = new HostRestrictions();
    hostRestrictions.allowPrivateAddresses = true;
    TestDownloadCrawlConsumer consumer =
        new TestDownloadCrawlConsumer(curator, archiveRepository, limits, hostRestrictions);
    URI url = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/archive.zip");
    CrawlJob crawlJob = new CrawlJob(datasetKey, EndpointType.DWC_ARCHIVE, url, 1, null);

    RuntimeException e =
        assertThrows(RuntimeException.class, () -> consumer.crawl(datasetKey, crawlJob));

    assertEquals(SizeLimitExceededException.class, e.getCause().getClass());
    assertEquals("good archive", Files.readString(localFile.toPath(), StandardCharsets.UTF_8));
    assertEquals(lastModified, localFile.lastModified());
    assertEquals(0, downloadFiles().length);
  }

  @Test
  public void testArchiveOverUnpackedLimitIsNotPromoted() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    File localFile = new File(archiveRepository, datasetKey + SUFFIX);
    Files.writeString(localFile.toPath(), "good archive");
    localFile.setLastModified(localFile.lastModified() - 3_600_000);

    SizeLimits limits = new SizeLimits();
    limits.maxUnpackedEntries = 1;
    HostRestrictions hostRestrictions = new HostRestrictions();
    hostRestrictions.allowPrivateAddresses = true;
    TestDownloadCrawlConsumer consumer =
        new TestDownloadCrawlConsumer(curator, archiveRepository, limits, hostRestrictions);
    URI url = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/twofiles.zip");
    CrawlJob crawlJob = new CrawlJob(datasetKey, EndpointType.DWC_ARCHIVE, url, 1, null);

    RuntimeException e =
        assertThrows(RuntimeException.class, () -> consumer.crawl(datasetKey, crawlJob));

    assertEquals(SizeLimitExceededException.class, e.getCause().getClass());
    assertEquals("good archive", Files.readString(localFile.toPath(), StandardCharsets.UTF_8));
    // The attempt is not marked as processed, so a retry with a higher limit unpacks it
    assertFalse(new File(archiveRepository, datasetKey + ".1" + SUFFIX).exists());
    assertEquals(0, downloadFiles().length);
  }

  private File[] downloadFiles() {
    return archiveRepository.listFiles((dir, name) -> name.endsWith(".download"));
  }

  private static class TestDownloadCrawlConsumer extends DownloadCrawlConsumer {
    TestDownloadCrawlConsumer(
        CuratorFramework curator,
        File archiveRepository,
        SizeLimits limits,
        HostRestrictions hostRestrictions) {
      super(curator, null, archiveRepository, 5_000, limits, hostRestrictions);
    }

    @Override
    protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
      return null;
    }

    @Override
    protected String getSuffix() {
      return SUFFIX;
    }

    @Override
    protected File getArchiveDirectory(File archiveRepository, UUID datasetKey) {
      return archiveRepository;
    }
  }
}
