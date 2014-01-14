package org.gbif.crawler.retry;

import org.gbif.crawler.RetryPolicy;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class RetryStrategyImplTest {

  @Test
  public void testTotalProtocolExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(2, 10, 10, 10);

    assertThat(retry.allowAfterProtocolException()).isTrue();
    assertThat(retry.allowAfterProtocolException()).isTrue();
    retry.successfulRequest();
    assertThat(retry.allowAfterProtocolException()).isFalse();
  }

  @Test
  public void testConsecutiveProtocolExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(10, 2, 10, 10);

    assertThat(retry.allowAfterProtocolException()).isTrue();
    retry.successfulRequest();
    assertThat(retry.allowAfterProtocolException()).isTrue();
    assertThat(retry.allowAfterProtocolException()).isTrue();
    assertThat(retry.allowAfterProtocolException()).isFalse();
  }

  @Test
  public void testTotalTransportExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(10, 10, 2, 10);

    assertThat(retry.allowAfterTransportException()).isTrue();
    assertThat(retry.allowAfterTransportException()).isTrue();
    retry.successfulRequest();
    assertThat(retry.allowAfterTransportException()).isFalse();
  }

  @Test
  public void testConsecutiveTransportExceptions() {
    RetryPolicy retry = new LimitedRetryPolicy(10, 10, 10, 2);

    assertThat(retry.allowAfterTransportException()).isTrue();
    retry.successfulRequest();
    assertThat(retry.allowAfterTransportException()).isTrue();
    assertThat(retry.allowAfterTransportException()).isTrue();
    assertThat(retry.allowAfterTransportException()).isFalse();
  }

  @Test
  public void testMixed() {
    RetryPolicy retry = new LimitedRetryPolicy(5, 2, 5, 2);

    assertThat(retry.allowAfterTransportException()).isTrue();  // TE 1
    assertThat(retry.allowAfterTransportException()).isTrue();  // TE 2
    assertThat(retry.allowAfterTransportException()).isFalse(); // TE 3 - consecutive exceptions reached
    retry.giveUpRequest();
    assertThat(retry.allowAfterProtocolException()).isTrue();   // PE 1
    retry.successfulRequest();
    retry.successfulRequest();
    assertThat(retry.allowAfterTransportException()).isTrue();  // TE 4
    assertThat(retry.allowAfterProtocolException()).isTrue();   // PE 2
    assertThat(retry.allowAfterTransportException()).isTrue();  // TE 5 - total exceptions reached
    retry.successfulRequest();
    assertThat(retry.allowAfterTransportException()).isFalse(); // TE 6
    assertThat(retry.allowAfterTransportException()).isFalse(); // TE 7
    assertThat(retry.allowAfterProtocolException()).isFalse();  // PE 3
    retry.successfulRequest();
    assertThat(retry.allowAfterTransportException()).isFalse(); // TE 8
    assertThat(retry.allowAfterProtocolException()).isFalse();  // PE 4
  }
}
