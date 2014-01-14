package org.gbif.crawler;

import java.util.Iterator;

/**
 * This is used to model a specific way to crawl a dataset.
 * <p/>
 * Depending on a context object it needs to provide another context that is then being processed. It can signal if
 * there is no more work to do or if a dataset has been fully crawled according to the requested specifications.
 * <p/>
 * Implementations are working with a {@link CrawlContext} object which may hold more state than needed for the
 * strategy. Implementations are not supposed to change that state. If an implementation needs to diverge from this
 * policy it must be documented.
 *
 * @param <CTX> the kind of context this strategy expects. It needs to be a subclass of {@link CrawlContext} because
 *              that supports paging which is something every single request might need no matter what the strategy is
 */
public interface CrawlStrategy<CTX extends CrawlContext> extends Iterator<CTX> {

  /**
   * Gets the next context to process.
   *
   * @return the next context
   *
   * @throws java.util.NoSuchElementException
   *          when there is no more work to process. You can avoid this by calling {@link #hasNext()} first
   */
  @Override
  CTX next();

}
