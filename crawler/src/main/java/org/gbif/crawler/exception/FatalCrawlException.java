package org.gbif.crawler.exception;

/**
 * When a component throws this exception it indicates that it encountered an error during crawler that is not
 * recoverable. The most sensible course of action to deal with this will usually be to abort the crawl.
 */
public class FatalCrawlException extends Exception {

  private static final long serialVersionUID = -1194110061370924754L;

  public FatalCrawlException() {
  }

  public FatalCrawlException(String message) {
    super(message);
  }

  public FatalCrawlException(String message, Throwable cause) {
    super(message, cause);
  }

  public FatalCrawlException(Throwable cause) {
    super(cause);
  }

}
