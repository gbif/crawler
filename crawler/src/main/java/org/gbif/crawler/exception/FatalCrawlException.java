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
package org.gbif.crawler.exception;

/**
 * When a component throws this exception it indicates that it encountered an error during crawler
 * that is not recoverable. The most sensible course of action to deal with this will usually be to
 * abort the crawl.
 */
public class FatalCrawlException extends Exception {

  private static final long serialVersionUID = -1194110061370924754L;

  public FatalCrawlException() {}

  public FatalCrawlException(String message) {
    super(message);
  }

  public FatalCrawlException(String message, Throwable cause) {
    super(message, cause);
  }

  public FatalCrawlException(Throwable cause) {
    super(cause);
  }
}
