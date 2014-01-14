package org.gbif.crawler.exception;

/**
 * Indicates an issue with the protocol handling, such as a response
 * in an unexpected format.
 * <p/>
 * The use of {@link java.net.ProtocolException} was
 * discarded, to differentiate that this deals with the harvesting protocol
 * such as DiGIR, BioCASe or TAPIR, and not underlying transport layer
 * protocols such as TCP, to which the {@link java.net.ProtocolException} is
 * typically associated.
 */
public class ProtocolException extends Exception {

  private static final long serialVersionUID = -4218627840124526625L;

  public ProtocolException(String message) {
    super(message);
  }

  public ProtocolException(Throwable cause) {
    super(cause);
  }

  public ProtocolException(String message, Throwable cause) {
    super(message, cause);
  }

}
