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
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import static org.gbif.crawler.CrawlerCoordinatorServiceImpl.EML_URL_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Covers {@link DownloadCrawlConsumer#downloadEndpointEml} against a local HTTP server. */
public class DownloadCrawlConsumerEndpointEmlTest {

  private static final String EML = "<eml:eml><dataset><title>Endpoint</title></dataset></eml:eml>";

  private static HttpServer server;
  private static String baseUrl;

  @TempDir File archiveRepository;

  @BeforeAll
  public static void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/eml.xml", exchange -> respond(exchange, 200, EML));
    server.createContext("/missing.xml", exchange -> respond(exchange, 404, "not found"));
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

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob(datasetKey, baseUrl + "/eml.xml"));

    File eml = EndpointEml.file(archiveRepository, datasetKey, 2);
    assertEquals(EML, Files.readString(eml.toPath()));
    assertEquals(eml, EndpointEml.find(archiveRepository, datasetKey, 2).orElse(null));
  }

  @Test
  public void testHttpErrorLeavesNoFile() {
    UUID datasetKey = UUID.randomUUID();

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob(datasetKey, baseUrl + "/missing.xml"));

    assertFalse(EndpointEml.file(archiveRepository, datasetKey, 2).exists());
  }

  @Test
  public void testUnreachableEndpointIsNotFatal() {
    UUID datasetKey = UUID.randomUUID();

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob(datasetKey, "http://127.0.0.1:1/eml.xml"));

    assertFalse(EndpointEml.file(archiveRepository, datasetKey, 2).exists());
  }

  @Test
  public void testNoEmlUrlDoesNothing() {
    UUID datasetKey = UUID.randomUUID();
    CrawlJob crawlJob =
      new CrawlJob(datasetKey, EndpointType.DWC_DP, URI.create(baseUrl + "/archive.zip"), 2, null);

    consumer().downloadEndpointEml(archiveRepository, datasetKey, crawlJob);

    assertFalse(EndpointEml.file(archiveRepository, datasetKey, 2).exists());
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
    return new DownloadCrawlConsumer(null, null, archiveRepository, 1000) {
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
    };
  }
}
