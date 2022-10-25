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
package org.gbif.crawler.xml.crawlserver.util;

import org.gbif.crawler.client.HttpCrawlClient;

public class HttpCrawlClientProvider {

  private static final int CONNECTION_TIMEOUT_MSEC = 600000; // 10 mins
  private static final int MAX_TOTAL_CONNECTIONS = 500;
  private static final int MAX_TOTAL_PER_ROUTE = 20;

  public static HttpCrawlClient newHttpCrawlClient() {
    return HttpCrawlClient.newInstance(
        CONNECTION_TIMEOUT_MSEC, MAX_TOTAL_CONNECTIONS, MAX_TOTAL_PER_ROUTE);
  }

  private HttpCrawlClientProvider() {
    throw new UnsupportedOperationException("Can't initialize class");
  }
}
