package org.gbif.crawler.strategy;

import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This implements a crawling strategy that iterates over scientific name ranges (even though scientific name is not
 * hardcoded in this class).  This class can run in one of 3 modes as documented below.  The default mode is the
 * original, "tried and tested" safe strategy using small ranges from Aaa-Aba, Aba-Abb etc.  For cases where one wishes
 * to be more aggressive, the ABC mode with do Aaa-Baa, Baa-Caa etc and to be super eager the AZ mode will try and do
 * Aaa-Zaa in one call.
 * <p/>
 * All modes will include a first search for everything before 'aaa' - i.e. {@code null} -> 'aaa' and similar at the
 * end with a 'Zaa' or 'Zza' -> {@code null}.
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

  public enum Mode {DEFAULT, ABC, AZ};
  private boolean isFirst = true;
  private final Mode mode;

  private final ScientificNameRangeCrawlContext context;

  public ScientificNameRangeStrategy(ScientificNameRangeCrawlContext context) {
    this(context,Mode.DEFAULT);
  }

  public ScientificNameRangeStrategy(ScientificNameRangeCrawlContext context, Mode mode) {
    this.context = checkNotNull(context, "context can't be null");
    this.mode = mode;
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

    // If we are at the very end we still need to search:
    // default mode: zza..null
    // others: zaa..null
    if ((mode == Mode.DEFAULT && context.getLowerBound().get().equals("Zza"))
      || (mode != Mode.DEFAULT && context.getLowerBound().get().equals("Zaa"))) {
      context.setUpperBound(null);
      return context;
    }

    // Otherwise increment normally as per Javadoc
    char[] chars = context.getUpperBound().get().toCharArray();
    if (mode == Mode.DEFAULT) {
      for (int pos = 1; pos >= 0; pos--) {
        if (chars[pos] == 'z') {
          chars[pos] = 'a';
        } else {
          chars[pos]++;
          break;
        }
      }
    } else if (mode == Mode.ABC) {
      chars[0]++; // Aaa - Baa, Baa - Caa etc

    } else {
      chars[0] = 'Z'; // upper goes straight to Z to achieve Aaa -> Zaa
    }

    context.setUpperBound(new String(chars));
    return context;
  }

  /**
   * We are at the end when the lower bound is the highest range (e.g. <em>Zzz</em> for default, <em>Zaa</em> for
   * others and the upper bound is {@code null}.
   *
   * @return {@code true} if there is more work pending
   */
  @Override
  public boolean hasNext() {
    return context.getUpperBound().isPresent();
  }

  /**
   * Visible to allow inspection of how e.g. a DiGIR crawl chooses to configure the range based on the declaredCount
   * of records which can be stored as a registry machine tag.
   */
  @VisibleForTesting
  public Mode getMode() {
    return mode;
  }
}
