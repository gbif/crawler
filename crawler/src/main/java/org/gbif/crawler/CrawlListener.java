package org.gbif.crawler;

import com.google.common.base.Optional;

/**
 * Used to listen for crawl events.
 *
 * Any exceptions thrown are logged but ignored for further processing.
 *
 * @param <CTX>  the crawl context this listener supports
 * @param <REQ>  the type of request this listener supports
 * @param <RESP> the type of response (from response handlers) this listener supports
 */
public interface CrawlListener<CTX extends CrawlContext, REQ, RESP> {

  /**
   * Reports that a Crawler will now actively begin to crawl.
   */
  void startCrawl();

  /**
   * Reports that we are making progress. This method may be called multiple times with the same context.
   *
   * @param context current context the crawler is working on
   */
  void progress(CTX context);

  /**
   * Reports the verbatim request as it will be sent to the host.
   *
   * @param req   the request
   * @param retry we might try a request multiple times if there was a recoverable error, this is the retry number
   *              beginning at 1
   */
  void request(REQ req, int retry);

  /**
   * Reports a response we received.
   *
   * @param response     we received
   * @param retry        which retry is this
   * @param duration     how long did it take to receive the response and process it
   * @param recordCount  how many records does the response claim to contain
   * @param endOfRecords are there more pages coming
   */
  void response(RESP response, int retry, long duration, Optional<Integer> recordCount, Optional<Boolean> endOfRecords);

  /**
   * Reports that we are done crawling a dataset and there were no errors that lead us to abort it prematurely.
   */
  void finishCrawlNormally();

  /**
   * The user requested us to abort the crawl.
   */
  void finishCrawlOnUserRequest();

  /**
   * Reports that the crawl is finished due to an abnormal reason (e.g. retries exhausted)
   */
  void finishCrawlAbnormally();

  void error(Throwable e);

  void error(String msg);

}
