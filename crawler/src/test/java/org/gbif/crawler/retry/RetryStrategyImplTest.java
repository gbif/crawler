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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RetryStrategyImplTest {

  @Test
  public void testTotalProtocolExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(2, 10, 10, 10);

    assertTrue(retry.allowAfterProtocolException());
    assertTrue(retry.allowAfterProtocolException());
    retry.successfulRequest();
    assertFalse(retry.allowAfterProtocolException());
  }

  @Test
  public void testConsecutiveProtocolExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(10, 2, 10, 10);

    assertTrue(retry.allowAfterProtocolException());
    retry.successfulRequest();
    assertTrue(retry.allowAfterProtocolException());
    assertTrue(retry.allowAfterProtocolException());
    assertFalse(retry.allowAfterProtocolException());
  }

  @Test
  public void testTotalTransportExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(10, 10, 2, 10);

    assertTrue(retry.allowAfterTransportException());
    assertTrue(retry.allowAfterTransportException());
    retry.successfulRequest();
    assertFalse(retry.allowAfterTransportException());
  }

  @Test
  public void testConsecutiveTransportExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(10, 10, 10, 2);

    assertTrue(retry.allowAfterTransportException());
    retry.successfulRequest();
    assertTrue(retry.allowAfterTransportException());
    assertTrue(retry.allowAfterTransportException());
    assertFalse(retry.allowAfterTransportException());
  }

  @Test
  public void testMixed() {
    RetryPolicy retry = new LimitedRetryPolicy(5, 2, 5, 2);

    assertTrue(retry.allowAfterTransportException()); // TE 1
    assertTrue(retry.allowAfterTransportException()); // TE 2
    assertFalse(retry.allowAfterTransportException()); // TE 3 - consecutive exceptions reached
    retry.giveUpRequest();
    assertTrue(retry.allowAfterProtocolException()); // PE 1
    retry.successfulRequest();
    retry.successfulRequest();
    assertTrue(retry.allowAfterTransportException()); // TE 4
    assertTrue(retry.allowAfterProtocolException()); // PE 2
    assertTrue(retry.allowAfterTransportException()); // TE 5 - total exceptions reached
    retry.successfulRequest();
    assertFalse(retry.allowAfterTransportException()); // TE 6
    assertFalse(retry.allowAfterTransportException()); // TE 7
    assertFalse(retry.allowAfterProtocolException()); // PE 3
    retry.successfulRequest();
    assertFalse(retry.allowAfterTransportException()); // TE 8
    assertFalse(retry.allowAfterProtocolException()); // PE 4
  }
}
