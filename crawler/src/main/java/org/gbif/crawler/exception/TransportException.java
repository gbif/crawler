package org.gbif.crawler.exception;

/**
 * Indicates an issue with the transport layer during communication.
 */
public class TransportException extends Exception {

  private static final long serialVersionUID = 5973132911743143191L;

  public TransportException(String message) {
    super(message);
  }

  public TransportException(Throwable cause) {
    super(cause);
  }

  public TransportException(String message, Throwable cause) {
    super(message, cause);
  }

}
