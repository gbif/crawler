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
package org.gbif.crawler;

import java.util.Optional;

/**
 * Simple abstract base listener so that you can only have to implement the methods you're
 * interested in.
 *
 * @see CrawlListener
 */
public class AbstractCrawlListener<CTX extends CrawlContext, REQ, RESP>
    implements CrawlListener<CTX, REQ, RESP> {

  private CTX currentContext;

  @Override
  public void startCrawl() {}

  @Override
  public void progress(CTX context) {
    currentContext = context;
  }

  @Override
  public void request(REQ req, int retry) {}

  @Override
  public void response(
      RESP response,
      int retry,
      long duration,
      Optional<Integer> recordCount,
      Optional<Boolean> endOfRecords) {}

  @Override
  public void finishCrawlNormally() {}

  @Override
  public void finishCrawlOnUserRequest() {}

  @Override
  public void finishCrawlAbnormally() {}

  @Override
  public void error(Throwable e) {}

  @Override
  public void error(String msg) {}

  public CTX getCurrentContext() {
    return currentContext;
  }
}
