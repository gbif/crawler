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
package org.gbif.crawler.retry;

import org.gbif.crawler.RetryPolicy;

/**
 * This implements a retry strategy that allows a certain fixed number of total and consecutive
 * exceptions.
 *
 * <p>The consecutive counts are reset with a successful request and the total number is never
 * reset. The counts are combined in the sense that if one of the consecutive limits is hit neither
 * is allowed to be retried and if any of the total limits have been hit the crawl should be
 * aborted.
 */
public class LimitedRetryPolicy implements RetryPolicy {

  // The maximum number of allowed total and consecutive Protocol and Transport exceptions
  private final int maxTotalProtocolExceptions;
  private final int maxConsecutiveProtocolExceptions;

  private final int maxTotalTransportExceptions;
  private final int maxConsecutiveTransportExceptions;

  // The current counts of total and consecutive exceptions
  private int totalProtocolExceptionCount;
  private int totalTransportExceptionCount;

  private int consecutiveProtocolExceptionCount;
  private int consecutiveTransportExceptionCount;

  public LimitedRetryPolicy(
      int maxTotalProtocolExceptions,
      int maxConsecutiveProtocolExceptions,
      int maxTotalTransportExceptions,
      int maxConsecutiveTransportExceptions) {
    this.maxTotalProtocolExceptions = maxTotalProtocolExceptions;
    this.maxConsecutiveProtocolExceptions = maxConsecutiveProtocolExceptions;
    this.maxTotalTransportExceptions = maxTotalTransportExceptions;
    this.maxConsecutiveTransportExceptions = maxConsecutiveTransportExceptions;
  }

  @Override
  public boolean allowAfterProtocolException() {
    totalProtocolExceptionCount++;
    consecutiveProtocolExceptionCount++;

    return allowRetry();
  }

  @Override
  public boolean allowAfterTransportException() {
    totalTransportExceptionCount++;
    consecutiveTransportExceptionCount++;

    return allowRetry();
  }

  @Override
  public void successfulRequest() {
    consecutiveProtocolExceptionCount = 0;
    consecutiveTransportExceptionCount = 0;
  }

  @Override
  public void giveUpRequest() {
    consecutiveProtocolExceptionCount = 0;
    consecutiveTransportExceptionCount = 0;
  }

  @Override
  public boolean abortCrawl() {
    return totalProtocolExceptionCount > maxTotalProtocolExceptions
        || totalTransportExceptionCount > maxTotalTransportExceptions;
  }

  private boolean allowRetry() {
    return !abortCrawl()
        && consecutiveProtocolExceptionCount <= maxConsecutiveProtocolExceptions
        && consecutiveTransportExceptionCount <= maxConsecutiveTransportExceptions;
  }
}
