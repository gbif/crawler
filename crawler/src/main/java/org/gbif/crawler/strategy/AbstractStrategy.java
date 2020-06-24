/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.strategy;

import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.CrawlStrategy;

/**
 * A base class for {@link CrawlStrategy} implementations.
 *
 * <p>Because {@link CrawlStrategy} extends {@link Iterable} we need to implement the {@link
 * #remove()} method but we don't expect to support it. This base class thus provides a default
 * implementation.
 *
 * @param <CTX> the kind of context this strategy expects. It needs to be a subclass of {@link
 *     org.gbif.crawler.CrawlContext} because that supports paging which is something every single
 *     request might need no matter what the strategy is
 */
public abstract class AbstractStrategy<CTX extends CrawlContext> implements CrawlStrategy<CTX> {

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
