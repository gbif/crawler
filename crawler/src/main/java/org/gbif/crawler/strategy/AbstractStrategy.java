package org.gbif.crawler.strategy;

import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.CrawlStrategy;

/**
 * A base class for {@link CrawlStrategy} implementations.
 * <p/>
 * Because {@link CrawlStrategy} extends {@link Iterable} we need to implement the {@link #remove()} method but we
 * don't expect to support it. This base class thus provides a default implementation.
 *
 * @param <CTX> the kind of context this strategy expects. It needs to be a subclass of {@link
 *              org.gbif.crawler.CrawlContext} because that supports paging which is something every single request
 *              might need no matter what the strategy is
 */
public abstract class AbstractStrategy<CTX extends CrawlContext> implements CrawlStrategy<CTX> {

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

}
