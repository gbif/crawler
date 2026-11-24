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

import org.gbif.utils.HttpClient;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;

import org.apache.http.StatusLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SizeLimitedHttpClientTest {

  private static final int BODY_SIZE = 10_000;

  @TempDir Path tempDir;

  private HttpServer server;

  @BeforeEach
  public void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    // Content-Length is sent
    server.createContext("/fixed", exchange -> respond(exchange, BODY_SIZE));
    // Chunked, no Content-Length
    server.createContext("/chunked", exchange -> respond(exchange, 0));
    server.start();
  }

  @AfterEach
  public void stopServer() {
    server.stop(0);
  }

  @Test
  public void testWithinLimit() throws Exception {
    HttpClient client = SizeLimitedHttpClient.newMultithreadedClient(5_000, 2, 2, BODY_SIZE);
    File file = tempDir.resolve("download").toFile();

    StatusLine status = client.downloadIfModifiedSince(url("/chunked"), null, file, true);

    assertEquals(200, status.getStatusCode());
    assertEquals(BODY_SIZE, file.length());
  }

  @Test
  public void testContentLengthOverLimit() throws Exception {
    HttpClient client = SizeLimitedHttpClient.newMultithreadedClient(5_000, 2, 2, BODY_SIZE - 1);
    File file = tempDir.resolve("download").toFile();

    assertThrows(
        SizeLimitExceededException.class,
        () -> client.downloadIfModifiedSince(url("/fixed"), null, file, true));
  }

  @Test
  public void testChunkedOverLimit() throws Exception {
    HttpClient client = SizeLimitedHttpClient.newMultithreadedClient(5_000, 2, 2, BODY_SIZE - 1);
    File file = tempDir.resolve("download").toFile();

    assertThrows(
        SizeLimitExceededException.class,
        () -> client.downloadIfModifiedSince(url("/chunked"), null, file, true));
  }

  @Test
  public void testNoLimit() throws Exception {
    HttpClient client = SizeLimitedHttpClient.newMultithreadedClient(5_000, 2, 2, 0);
    File file = tempDir.resolve("download").toFile();

    client.downloadIfModifiedSince(url("/fixed"), null, file, true);

    assertEquals(BODY_SIZE, file.length());
  }

  private URL url(String path) throws Exception {
    return new URL("http://127.0.0.1:" + server.getAddress().getPort() + path);
  }

  private static void respond(HttpExchange exchange, long length) throws IOException {
    exchange.sendResponseHeaders(200, length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(new byte[BODY_SIZE]);
    }
  }
}
