package org.gbif.crawler;

import com.google.common.base.Optional;

/**
 * Simple abstract base listener so that you can only have to implement the methods you're interested in.
 *
 * @see CrawlListener
 */
public class AbstractCrawlListener<CTX extends CrawlContext, REQ, RESP> implements CrawlListener<CTX, REQ, RESP> {

  private CTX currentContext;

  @Override
  public void startCrawl() {
  }

  @Override
  public void progress(CTX context) {
    currentContext = context;
  }

  @Override
  public void request(REQ req, int retry) {
  }

  @Override
  public void response(
    RESP response, int retry, long duration, Optional<Integer> recordCount, Optional<Boolean> endOfRecords
  ) {
  }

  @Override
  public void finishCrawlNormally() {
  }

  @Override
  public void finishCrawlOnUserRequest() {
  }

  @Override
  public void finishCrawlAbnormally() {
  }

  @Override
  public void error(Throwable e) {
  }

  @Override
  public void error(String msg) {
  }

  public CTX getCurrentContext() {
    return currentContext;
  }
}
