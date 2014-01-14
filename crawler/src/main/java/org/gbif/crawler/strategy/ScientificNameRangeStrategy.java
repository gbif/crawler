package org.gbif.crawler.strategy;

import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This implements a crawling strategy that iterates over scientific name ranges (even though scientific name is not
 * hardcoded in this class).
 * <p/>
 * The strategy is to first search everything before 'aaa' and from there we always increment
 * the second character: {@code null} -> 'aaa', 'aaa' -> 'aba', 'aba' -> 'aca', ..., 'aza' -> 'baa', ..., 'zzz' ->
 * {@code null}.
 * <p/>
 * We use three character strings even though we don't change the third because a lot of provides require at least that
 * many characters for a search.
 * <p/>
 * <em>Note:</em> There is currently no way of stopping the job automatically when a certain range has been scanned.
 * The initial context is just taken as a starting point buf from there on all the other ranges will be emitted.
 * <p/>
 * This class is not thread-safe.
 */
@NotThreadSafe
public class ScientificNameRangeStrategy extends AbstractStrategy<ScientificNameRangeCrawlContext> {

  private boolean isFirst = true;

  private final ScientificNameRangeCrawlContext context;

  public ScientificNameRangeStrategy(ScientificNameRangeCrawlContext context) {
    this.context = checkNotNull(context, "context can't be null");
  }

  @Override
  public ScientificNameRangeCrawlContext next() {
    if (!hasNext()) {
      throw new NoSuchElementException("There are no more elements in this crawl range");
    }

    // If this is our first request we just return the context as we got it because we don't want to lose that first
    // step which usually is null..aaa
    if (isFirst) {
      isFirst = false;
      return context;
    }

    // We start with the last upper bound
    context.setLowerBound(context.getUpperBound().get());

    // If we are at the very end we still need to search zzz..null
    if (context.getLowerBound().get().equals("Zza")) {
      context.setUpperBound(null);
      return context;
    }

    // Otherwise increment normally as per Javadoc
    char[] chars = context.getUpperBound().get().toCharArray();
    for (int pos = 1; pos >= 0; pos--) {
      if (chars[pos] == 'z') {
        chars[pos] = 'a';
      } else {
        chars[pos]++;
        break;
      }
    }

    context.setUpperBound(new String(chars));
    return context;
  }

  /**
   * We are at the end when the lower bound is <em>Zzz</em> and the upper bound is {@code null}.
   *
   * @return {@code true} if there is more work pending
   */
  @Override
  public boolean hasNext() {
    return context.getUpperBound().isPresent();
  }

}
