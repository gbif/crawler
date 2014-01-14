package org.gbif.crawler;

/**
 * Indicates that we couldn't initiate a crawl because it is already running.
 */
public class AlreadyCrawlingException extends RuntimeException {

  private static final long serialVersionUID = 380123546072253442L;

  public AlreadyCrawlingException() {
  }

  public AlreadyCrawlingException(String message) {
    super(message);
  }

  public AlreadyCrawlingException(Throwable cause) {
    super(cause);
  }

  public AlreadyCrawlingException(String message, Throwable cause) {
    super(message, cause);
  }

}
