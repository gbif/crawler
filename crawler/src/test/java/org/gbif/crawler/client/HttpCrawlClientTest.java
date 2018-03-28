package org.gbif.crawler.client;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
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
import org.apache.http.conn.ClientConnectionManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
// Mockito generics appears insurmountable
@SuppressWarnings("unchecked")
public class HttpCrawlClientTest {

  private static final String TEST_URL = "http://www.gbif-uat.org";

  @Mock HttpClientConnectionManager connectionManager;
  @Mock HttpClient httpClient;
  @Mock ResponseHandler<HttpResponse, String> innerResponseHandler = mock(ResponseHandler.class);

  @Test
  public void testSuccess() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    ArgumentCaptor<HttpGet> captor = ArgumentCaptor.forClass(HttpGet.class);
    when(httpClient.execute(captor.capture(), any(org.apache.http.client.ResponseHandler.class))).thenReturn("foo");
    assertThat(client.execute(TEST_URL, innerResponseHandler)).isEqualTo("foo");
    assertThat(captor.getValue().getURI().toString()).isEqualTo(TEST_URL);
  }

  @Test(expected = TransportException.class)
  public void testTransportException1() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
      .thenThrow(new ClientProtocolException("cpe"));

    client.execute(TEST_URL, innerResponseHandler);
  }

  @Test(expected = TransportException.class)
  public void testTransportException2() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
      .thenThrow(new RuntimeException(new TransportException("inner transport exception")));

    client.execute(TEST_URL, innerResponseHandler);
  }

  @Test(expected = ProtocolException.class)
  public void testProtocolException() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
      .thenThrow(new RuntimeException(new ProtocolException("inner protocol exception")));

    client.execute(TEST_URL, innerResponseHandler);
  }

  @Test(expected = FatalCrawlException.class)
  public void testAbortException() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
      .thenThrow(new RuntimeException(new FatalCrawlException("inner abort exception")));

    client.execute(TEST_URL, innerResponseHandler);
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimeException() throws Exception {
    HttpCrawlClient client = new HttpCrawlClient(connectionManager, httpClient);

    when(httpClient.execute(any(HttpGet.class), any(org.apache.http.client.ResponseHandler.class)))
      .thenThrow(new RuntimeException("plain runtime exception"));

    client.execute(TEST_URL, innerResponseHandler);
  }


  @Test
  public void testConstructor() {
    try {
      new HttpCrawlClient(null, null);
      fail();
    } catch (Exception e) {
    }

    try {
      new HttpCrawlClient(mock(HttpClientConnectionManager.class), null);
      fail();
    } catch (Exception e) {
    }

    try {
      new HttpCrawlClient(null, mock(HttpClient.class));
      fail();
    } catch (Exception e) {
    }

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
      new HttpCrawlClient.ForwardingResponseHandler<String>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenReturn("FOO");

    assertThat(responseHandler.handleResponse(response)).isEqualTo("FOO");
  }

  @Test(expected = ClientProtocolException.class)
  public void testForwardingResponseHandlerBadResponse() throws IOException, ProtocolException, TransportException {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
      new HttpCrawlClient.ForwardingResponseHandler<String>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_FORBIDDEN);
    responseHandler.handleResponse(response);
  }

  @Test(expected = RuntimeException.class)
  public void testForwardingResponseHandlerException1() throws Exception {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
      new HttpCrawlClient.ForwardingResponseHandler<String>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenThrow(new TransportException("FOO"));

    responseHandler.handleResponse(response);
  }

  @Test(expected = RuntimeException.class)
  public void testForwardingResponseHandlerException2() throws Exception {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
      new HttpCrawlClient.ForwardingResponseHandler<String>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenThrow(new ProtocolException("FOO"));

    responseHandler.handleResponse(response);
  }

  @Test(expected = RuntimeException.class)
  public void testForwardingResponseHandlerException3() throws Exception {
    HttpCrawlClient.ForwardingResponseHandler<String> responseHandler =
      new HttpCrawlClient.ForwardingResponseHandler<String>(innerResponseHandler);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(innerResponseHandler.handleResponse(response)).thenThrow(new FatalCrawlException("FOO"));

    responseHandler.handleResponse(response);
  }

}
