package org.gbif.crawler.xml.crawlserver.util;

import org.gbif.crawler.client.HttpCrawlClient;

import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

public class HttpCrawlClientProvider {

  private static final int DEFAULT_HTTP_PORT = 80;

  private static final int CONNECTION_TIMEOUT_MSEC = 600000; // 10 mins
  private static final int MAX_TOTAL_CONNECTIONS = 500;
  private static final int MAX_TOTAL_PER_ROUTE = 20;

  public static HttpCrawlClient newHttpCrawlClient() {
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(new Scheme("http", DEFAULT_HTTP_PORT, PlainSocketFactory.getSocketFactory()));

    PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager(schemeRegistry);
    connectionManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
    connectionManager.setDefaultMaxPerRoute(MAX_TOTAL_PER_ROUTE);

    HttpParams params = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(params, CONNECTION_TIMEOUT_MSEC);
    HttpConnectionParams.setSoTimeout(params, CONNECTION_TIMEOUT_MSEC);
    params.setLongParameter(ClientPNames.CONN_MANAGER_TIMEOUT, CONNECTION_TIMEOUT_MSEC);
    HttpClient httpClient = new DecompressingHttpClient(new DefaultHttpClient(connectionManager, params));

    return new HttpCrawlClient(connectionManager, httpClient);
  }

  private HttpCrawlClientProvider() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

}
