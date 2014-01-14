package org.gbif.crawler.protocol.biocase;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class BiocaseResponseHandlerTest {

  @Test
  public void testConstructor() {
    BiocaseResponseHandler handler = new BiocaseResponseHandler();

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
