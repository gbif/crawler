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
package org.gbif.crawler.client;

import org.gbif.crawler.ResponseHandler;
import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.exception.TransportException;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpClientConnectionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Mockito generics appears insurmountable
@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
public class HttpCrawlClientTest {

  private static final String TEST_URL = "http://www.gbif-uat.org";

  @Mock HttpClientConnectionManager connectionManager;
  @Mock HttpClient httpClient;
  @Mock ResponseHandler<HttpResponse, String> innerResponseHandler = mock(ResponseHandler.class);

  @Test
  public void testSuccess() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    ArgumentCaptor<HttpGet> captor = ArgumentCaptor.forClass(HttpGet.class);
    when(httpClient.execute(captor.capture(), any(org.apache.http.client.ResponseHandler.class)))
        .thenReturn("foo");
    assertEquals("foo", client.execute(TEST_URL, innerResponseHandler));
    assertEquals(TEST_URL, captor.getValue().getURI().toString());
  }

  @Test
  public void testTransportException1() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
        .thenThrow(new ClientProtocolException("cpe"));

    assertThrows(TransportException.class, () -> client.execute(TEST_URL, innerResponseHandler));
  }

  @Test
  public void testTransportException2() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
        .thenThrow(new RuntimeException(new TransportException("inner transport exception")));

    assertThrows(TransportException.class, () -> client.execute(TEST_URL, innerResponseHandler));
  }

  @Test
  public void testProtocolException() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
        .thenThrow(new RuntimeException(new ProtocolException("inner protocol exception")));

    assertThrows(ProtocolException.class, () -> client.execute(TEST_URL, innerResponseHandler));
  }

  @Test
  public void testAbortException() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
        .thenThrow(new RuntimeException(new FatalCrawlException("inner abort exception")));

    assertThrows(FatalCrawlException.class, () -> client.execute(TEST_URL, innerResponseHandler));
  }

  @Test
  public void testRuntimeException() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
        .thenThrow(new RuntimeException("plain runtime exception"));

    assertThrows(RuntimeException.class, () -> client.execute(TEST_URL, innerResponseHandler));
  }

  @Test
  public void testConstructor() {
    assertThrows(Exception.class, () -> new HttpCrawlClient(null, null));
    assertThrows(Exception.class, () -> new HttpCrawlClient(mock(HttpClientConnectionManager.class), null));
    assertThrows(Exception.class, () -> new HttpCrawlClient(null, mock(HttpClient.class)));
  }

  @Test
  public void testShutdown() {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    client.shutdown();

    verify(connectionManager).shutdown();
  }

  @Test
  public void testForwardingResponseHandler()
      throws IOException, ProtocolException, TransportException, FatalCrawlException {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
        new HttpCrawlClient.ForwardingResponseHandler<>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenReturn("FOO");

    assertEquals("FOO", responseHandler.handleResponse(response));
  }

  @Test
  public void testForwardingResponseHandlerBadResponse() {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
        new HttpCrawlClient.ForwardingResponseHandler<>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_FORBIDDEN);
    assertThrows(ClientProtocolException.class, () -> responseHandler.handleResponse(response));
  }

  @Test
  public void testForwardingResponseHandlerException1() throws Exception {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
        new HttpCrawlClient.ForwardingResponseHandler<>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenThrow(new TransportException("FOO"));

    assertThrows(RuntimeException.class, () -> responseHandler.handleResponse(response));
  }

  @Test
  public void testForwardingResponseHandlerException2() throws Exception {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
        new HttpCrawlClient.ForwardingResponseHandler<>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenThrow(new ProtocolException("FOO"));

    assertThrows(RuntimeException.class, () -> responseHandler.handleResponse(response));
  }

  @Test
  public void testForwardingResponseHandlerException3() throws Exception {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
        new HttpCrawlClient.ForwardingResponseHandler<>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenThrow(new FatalCrawlException("FOO"));

    assertThrows(RuntimeException.class, () -> responseHandler.handleResponse(response));
  }
}
