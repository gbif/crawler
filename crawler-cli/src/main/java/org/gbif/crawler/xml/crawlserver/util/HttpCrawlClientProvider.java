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

  private static final int CONNECTION_TIMEOUT_MSEC = 600000; // 10 mins
  private static final int MAX_TOTAL_CONNECTIONS = 500;
  private static final int MAX_TOTAL_PER_ROUTE = 20;

  public static HttpCrawlClient newHttpCrawlClient() {
    return HttpCrawlClient.newInstance(CONNECTION_TIMEOUT_MSEC, MAX_TOTAL_CONNECTIONS, MAX_TOTAL_PER_ROUTE);
  }

  private HttpCrawlClientProvider() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

}
