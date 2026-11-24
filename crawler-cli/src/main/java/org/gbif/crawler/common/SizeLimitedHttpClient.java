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
import org.gbif.utils.HttpUtil;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;

/**
 * Builds a {@link HttpClient} configured like {@code HttpUtil.newMultithreadedClient}, which fails
 * any response body larger than a maximum number of bytes, and resolves host names with the given
 * {@link DnsResolver}.
 */
public final class SizeLimitedHttpClient {

  private SizeLimitedHttpClient() {}

  /**
   * @param timeout in milliseconds
   * @param maxBytes maximum response body size, 0 for no limit
   */
  public static HttpClient newMultithreadedClient(
      int timeout, int maxConnections, int maxPerRoute, long maxBytes, DnsResolver dnsResolver) {
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setSocketTimeout(timeout)
            .setConnectTimeout(timeout)
            .setConnectionRequestTimeout(timeout)
            .build();

    Registry<ConnectionSocketFactory> registry =
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.INSTANCE)
            .register("https", new SSLConnectionSocketFactory(SSLContexts.createSystemDefault()))
            .build();
    PoolingHttpClientConnectionManager connectionManager =
        new PoolingHttpClientConnectionManager(registry, dnsResolver);
    connectionManager.setDefaultConnectionConfig(
        ConnectionConfig.custom().setCharset(StandardCharsets.UTF_8).build());
    connectionManager.setMaxTotal(maxConnections);
    connectionManager.setDefaultMaxPerRoute(maxPerRoute);

    HttpClientBuilder builder =
        HttpClientBuilder.create()
            // Retain compressed content, e.g. a tar.gz archive we download
            .disableContentCompression()
            .setRedirectStrategy(new PermanentRedirectStrategy())
            .setDefaultRequestConfig(requestConfig)
            .setConnectionManager(connectionManager)
            .setUserAgent(userAgent(timeout, maxConnections, maxPerRoute));
    if (maxBytes > 0) {
      builder.addInterceptorLast(new ResponseSizeLimitInterceptor(maxBytes));
    }
    CloseableHttpClient client = builder.build();

    return new HttpClient(client, requestConfig);
  }

  /** Same user agent as {@code HttpUtil}, so publishers see no change. */
  private static String userAgent(int timeout, int maxConnections, int maxPerRoute) {
    Package utils = HttpUtil.class.getPackage();
    String version =
        utils.getImplementationVersion() == null ? "development" : utils.getImplementationVersion();
    return String.format(
        "%s/%s (Java/%s; M-%d-%d-%d; +https://www.gbif.org/)",
        utils.getName().replace("gbif", "GBIF"),
        version,
        System.getProperty("java.version"),
        timeout,
        maxConnections,
        maxPerRoute);
  }

  /** Also follows 308 redirects, as {@code HttpUtil} does. */
  private static class PermanentRedirectStrategy extends DefaultRedirectStrategy {
    @Override
    public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context)
        throws ProtocolException {
      return super.isRedirected(request, response, context)
          || (response.getStatusLine().getStatusCode() == 308
              && isRedirectable(request.getRequestLine().getMethod()));
    }
  }

  /** Rejects or truncates with an error any response body over the limit. */
  static class ResponseSizeLimitInterceptor implements HttpResponseInterceptor {

    private final long maxBytes;

    ResponseSizeLimitInterceptor(long maxBytes) {
      this.maxBytes = maxBytes;
    }

    @Override
    public void process(HttpResponse response, HttpContext context) throws IOException {
      HttpEntity entity = response.getEntity();
      if (entity == null) {
        return;
      }
      if (entity.getContentLength() > maxBytes) {
        throw new SizeLimitExceededException(
            "Response of " + entity.getContentLength() + " bytes is over the limit of " + maxBytes);
      }
      response.setEntity(new LimitedEntity(entity, maxBytes));
    }
  }

  private static class LimitedEntity extends HttpEntityWrapper {

    private final long maxBytes;

    LimitedEntity(HttpEntity entity, long maxBytes) {
      super(entity);
      this.maxBytes = maxBytes;
    }

    @Override
    public InputStream getContent() throws IOException {
      return new LimitedInputStream(super.getContent(), maxBytes);
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
      try (InputStream in = getContent()) {
        in.transferTo(out);
      }
    }
  }

  private static class LimitedInputStream extends FilterInputStream {

    private final long maxBytes;
    private long count;

    LimitedInputStream(InputStream in, long maxBytes) {
      super(in);
      this.maxBytes = maxBytes;
    }

    @Override
    public int read() throws IOException {
      int b = super.read();
      if (b != -1) {
        add(1);
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = super.read(b, off, len);
      if (read > 0) {
        add(read);
      }
      return read;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = super.skip(n);
      if (skipped > 0) {
        add(skipped);
      }
      return skipped;
    }

    private void add(long read) throws SizeLimitExceededException {
      count += read;
      if (count > maxBytes) {
        throw new SizeLimitExceededException("Response is over the limit of " + maxBytes + " bytes");
      }
    }
  }
}
