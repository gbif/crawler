package org.gbif.crawler;

/**
 * This builds a protocol specific request for a certain crawl context.
 * <p/>
 * One example would be a request for a scientific name range for BioCASe.
 *
 * @param <CTX> type of context to handle. This could be a scientific name range filter or a request without any filter
 * @param <REQ> type of the request that is being created. This is configurable so we can allow direct construction of
 *              a complex query object if necessary but is expected to be mostly {@link String} for now.
 */
public interface RequestHandler<CTX extends CrawlContext, REQ> {

  /**
   * Builds a request for a context.
   * <p/>
   * This is dependent on the protocol being used as well as the {@link CrawlStrategy} through its {@link CrawlContext}
   * implementation.
   *
   * @param context to build the request for
   *
   * @return request
   */
  REQ buildRequestUrl(CTX context);

  /**
   * Returns the number of records that this request handler will request at a time.
   *
   * @return number of records this request handler will request
   */
  int getLimit();

}
