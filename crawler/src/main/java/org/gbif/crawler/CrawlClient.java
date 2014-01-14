package org.gbif.crawler;

import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.exception.TransportException;

/**
 * Implementations of this interface take responsibility for interactions with endpoints. They take some form of
 * request, a handler that processes the response of this request into a result.
 *
 * @param <REQUEST>  format of the request to process. This is expected to be mostly a String but could be a complex
 *                   object if needed
 * @param <RESPONSE> the type of the response that the CrawlClient produces which the ResponseHandler will have to
 *                   process
 */
public interface CrawlClient<REQUEST, RESPONSE> {

  /**
   * Executes the request and lets a ResponseHandler handle it and returns its result.
   *
   * @param request that should be issued
   * @param handler to process the response and that produces the result
   *
   * @throws TransportException on transport error, such as timeout, unable to connect etc.
   * @throws ProtocolException  on error during handling of the response (e.g. such as a unexpected format, XML issues)
   */
  <RESULT> RESULT execute(REQUEST request, ResponseHandler<RESPONSE, RESULT> handler)
    throws FatalCrawlException, TransportException, ProtocolException;

}
