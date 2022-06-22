/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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

import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.exception.TransportException;

import java.util.Optional;

/**
 * Implementations of this interface will handle the response received by {@link CrawlClient}s and
 * produce a result but it also plays a part in the paging through resources.
 *
 * <p>They have three jobs:
 *
 * <ul>
 *   <li>Convert the response into a format that is suitable for shipping as a message
 *   <li>Extract information from the response that is important for paging
 *   <li>Extract enough information from the response to see if it is a duplicate response
 * </ul>
 *
 * @param <RESPONSE> the type of response to handle. This is dependent on the {@link CrawlClient}
 *     implementation
 * @param <RESULT> the type of result this response handler produces when processing a response
 */
public interface ResponseHandler<RESPONSE, RESULT> {

  /**
   * Handles a response and produces a result.
   *
   * @param response to be handled
   * @throws ProtocolException Implementation specific
   * @throws TransportException Implementation specific
   */
  RESULT handleResponse(RESPONSE response)
      throws FatalCrawlException, ProtocolException, TransportException;

  /**
   * This can be used to see if this response handler is in an illegal state or not. This can happen
   * after parsing a wrong response or if there has been a transport exception. A proper response
   * needs to be handled for this to be cleared.
   *
   * @return false if the other getter methods will throw a {@link IllegalStateException}
   */
  boolean isValidState();

  /** @return the number of records in this page */
  Optional<Integer> getRecordCount();

  /** @return true if there are more records to extract */
  Optional<Boolean> isEndOfRecords();

  /** @return a hash value for the current page. This can be used to detect duplicate pages. */
  Optional<Long> getContentHash();
}
