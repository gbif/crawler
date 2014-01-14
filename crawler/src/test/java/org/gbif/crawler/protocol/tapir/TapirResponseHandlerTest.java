package org.gbif.crawler.protocol.tapir;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class TapirResponseHandlerTest {

  @Test
  public void testConstructor() {
    TapirResponseHandler handler = new TapirResponseHandler();

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
