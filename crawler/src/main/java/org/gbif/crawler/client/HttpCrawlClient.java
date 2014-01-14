package org.gbif.crawler.client;

import org.gbif.crawler.CrawlClient;
import org.gbif.crawler.ResponseHandler;
import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.exception.TransportException;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Executes HTTP GET requests in String form using Apache HttpClient.
 * <p/>
 * This client does not support HTTPS.
 * <p/>
 * {@link ResponseHandler}s must understand how to process a {@link HttpResponse} but they do not need to handle stream
 * closing as this is done by this class. When not needed anymore {@link #shutdown()} should be called so underlying
 * resources (e.g. the connection manager) can be closed.
 * <p/>
 * This class is thread-safe.
 */
// TODO: Log redirects - implement a RedirectStrategy
// TODO: Automatic retries
// TODO: Add HTTPS support
@ThreadSafe
public class HttpCrawlClient implements CrawlClient<String, HttpResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpCrawlClient.class);

  private final ClientConnectionManager connectionManager;
  private final HttpClient httpClient;

  /**
   * Factory method to create a new instance of this client with a few default settings. This creates an instance that
   * works with HTTP only (no HTTPS), uses multi-threading for its connections, supports compressed content and sets
   * some user provided timeouts.
   *
   * @param connectionTimeout   this timeout in milliseconds is used for establishing a connection and for waiting for
   *                            data
   * @param maxTotalConnections maximum total number of connections
   * @param maxTotalPerRoute    maximum number of connections per host
   *
   * @return new HttpCrawlClient with default settings
   */
  public static HttpCrawlClient newInstance(int connectionTimeout, int maxTotalConnections, int maxTotalPerRoute) {
    checkArgument(connectionTimeout > 0, "connectionTimeout has to be greater than zero");
    checkArgument(maxTotalConnections > 0, "maxTotalConnections has to be greater than zero");
    checkArgument(maxTotalPerRoute > 0, "maxTotalPerRoute has to be greater than zero");

    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));

    PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager(schemeRegistry);
    connectionManager.setMaxTotal(maxTotalConnections);
    connectionManager.setDefaultMaxPerRoute(maxTotalPerRoute);

    HttpParams params = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(params, connectionTimeout);
    HttpConnectionParams.setSoTimeout(params, connectionTimeout);
    params.setLongParameter(ClientPNames.CONN_MANAGER_TIMEOUT, connectionTimeout);
    HttpClient httpClient = new DecompressingHttpClient(new DefaultHttpClient(connectionManager, params));

    return new HttpCrawlClient(connectionManager, httpClient);
  }

  public HttpCrawlClient(ClientConnectionManager connectionManager, HttpClient httpClient) {
    Preconditions.checkNotNull(connectionManager);
    Preconditions.checkNotNull(httpClient);

    this.connectionManager = connectionManager;
    this.httpClient = httpClient;
  }

  /**
   * Initiates the request using HTTP and processes the response with the given Response handler.
   */
  @Override
  public <RESULT> RESULT execute(String request, final ResponseHandler<HttpResponse, RESULT> handler)
    throws FatalCrawlException, TransportException, ProtocolException {
    HttpUriRequest httpget = new HttpGet(request);
    LOG.debug("Executing request [{}]", request);
    try {
      /*
        This actually executes the request which can throw either a IOException (or its subclass
        ClientProtocolException) or the special runtime exception (InnerResponseHandlerException) from the
        ForwardingResponseHandler. We assume that all exceptions coming from the HttpClient itself are
        protocol level exceptions.
       */
      return httpClient.execute(httpget, new ForwardingResponseHandler<RESULT>(handler));
    } catch (RuntimeException e) {
      LOG.debug("Caught exception from underlying ResponseHandler", e.getCause());

      Throwables.propagateIfPossible(e.getCause(), ProtocolException.class);
      Throwables.propagateIfPossible(e.getCause(), TransportException.class);
      Throwables.propagateIfPossible(e.getCause(), FatalCrawlException.class);
      throw e;
    } catch (IOException e) {
      LOG.debug("Caught exception during the HTTP request [{}]", httpget, e);
      // Both ClientProtocolException as well as IOException are TransportExceptions
      throw new TransportException(e);
    }
  }

  /**
   * Should be called when this object is not needed anymore so that resources can be closed.
   */
  // TODO: Introduce a state and throw IllegalStateException if it is shut down
  public void shutdown() {
    connectionManager.shutdown();
  }

  /**
   * An implementation of HTTPClient's ResponseHandler interface which forwards to our own ResponseHandler interface.
   * <p/>
   * This automatically checks for failed requests (i.e. HTTP response code != 200).
   *
   * @param <R> the return type of our ResponseHandler
   */
  @VisibleForTesting
  static class ForwardingResponseHandler<R> implements org.apache.http.client.ResponseHandler<R> {

    private final ResponseHandler<HttpResponse, R> handler;

    ForwardingResponseHandler(ResponseHandler<HttpResponse, R> handler) {
      this.handler = handler;
    }

    /**
     * Handles a response by forwarding it to an inner response handler.
     */
    @Override
    public R handleResponse(HttpResponse response) throws IOException {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new ClientProtocolException(
          "HTTP status code[" + response.getStatusLine().getStatusCode() + "] indicates failure: [" + response
            .getStatusLine() + "]");
      }

      try {
        return handler.handleResponse(response);
      } catch (ProtocolException e) {
        throw Throwables.propagate(e);
      } catch (TransportException e) {
        throw Throwables.propagate(e);
      } catch (FatalCrawlException e) {
        throw Throwables.propagate(e);
      }
    }

  }

}

