package org.gbif.crawler;

/**
 * Will be called by the crawler to determine if certain operations should be retried after errors during the crawl.
 */
public interface RetryPolicy {

  /**
   * Call this method after every {@link org.gbif.crawler.exception.ProtocolException} to determine if the operation
   * should be retried.
   *
   * @return if the operation in question should be retried
   */
  boolean allowAfterProtocolException();

  /**
   * Call this method after every {@link org.gbif.crawler.exception.TransportException} to determine if the operation
   * should be retried.
   *
   * @return if the operation in question should be retried
   */
  boolean allowAfterTransportException();

  /**
   * Called to register a successful request. This can be used by implementations to reset internal counters.
   */
  void successfulRequest();

  /**
   * Called to let the strategy know that we've given up the current request and will try the next if allowed.
   */
  void giveUpRequest();

  /**
   * Used to see if the crawl should be aborted.
   *
   * @return {@code true} if the crawl should be aborted
   */
  boolean abortCrawl();

}
