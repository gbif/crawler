package org.gbif.crawler.protocol.digir;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class DigirResponseHandlerTest {

  @Test
  public void testConstructor() {
    DigirResponseHandler handler = new DigirResponseHandler();

    assertThat(handler.isValidState()).isFalse();

    try {
      handler.isEndOfRecords();
      fail();
    } catch (IllegalStateException e) {
    }

    try {
      handler.getContentHash();
      fail();
    } catch (IllegalStateException e) {
    }

    try {
      handler.getRecordCount();
      fail();
    } catch (IllegalStateException e) {
    }

  }

}
