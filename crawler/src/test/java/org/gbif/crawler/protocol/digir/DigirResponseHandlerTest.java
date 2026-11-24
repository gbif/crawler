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
package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.exception.ProtocolException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.common.io.Resources;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class DigirResponseHandlerTest {

  private static final String XML_DECLARATION = "<?xml version='1.0' encoding='utf-8' ?>";

  @Test
  public void testConstructor() {
    DigirResponseHandler handler = new DigirResponseHandler();

    assertFalse(handler.isValidState());
    assertThrows(IllegalStateException.class, handler::isEndOfRecords);
    assertThrows(IllegalStateException.class, handler::getContentHash);
    assertThrows(IllegalStateException.class, handler::getRecordCount);
  }

  @Test
  public void testExternalEntityIsNotResolved(@TempDir Path tempDir) throws Exception {
    Path secret = Files.writeString(tempDir.resolve("secret.txt"), "secret");
    String xml =
        fixture()
            .replace(
                XML_DECLARATION,
                XML_DECLARATION
                    + "<!DOCTYPE response [<!ENTITY xxe SYSTEM '"
                    + secret.toUri()
                    + "'>]>")
            .replace("<version>", "<version>&xxe;");

    DigirResponseHandler handler = new DigirResponseHandler();
    assertThrows(ProtocolException.class, () -> handler.handleResponse(response(xml)));
  }

  @Test
  public void testExternalDtdIsNotLoaded() throws Exception {
    // Nothing listens on port 9, so loading the DTD would fail the request
    String xml =
        fixture()
            .replace(
                XML_DECLARATION,
                XML_DECLARATION + "<!DOCTYPE response SYSTEM 'http://127.0.0.1:9/evil.dtd'>");

    DigirResponseHandler handler = new DigirResponseHandler();
    handler.handleResponse(response(xml));
    assertTrue(handler.isValidState());
  }

  private static String fixture() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("org/gbif/crawler/protocol/digir/response_arizona.xml"),
            StandardCharsets.UTF_8);
    assertTrue(xml.startsWith(XML_DECLARATION));
    return xml;
  }

  private static HttpResponse response(String xml) throws IOException {
    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getEntity().getContent())
        .thenReturn(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    return response;
  }
}
