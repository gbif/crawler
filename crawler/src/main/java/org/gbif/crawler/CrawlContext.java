package org.gbif.crawler;

import javax.validation.constraints.Min;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for any crawl context object. These objects are supposed to hold the mutable state of a running crawl
 * which is needed for the specific strategy in question (e.g. upper and lower bounds for scientific name range based
 * crawls).
 * <p/>
 * Every crawl we do will always have to support paging and we need an offset for that. This offset needs to be greater
 * than or equal to <em>0</em>.
 */
public class CrawlContext {

  @Min(0)
  private int offset;

  private boolean speculative;

  private Optional<Long> lastContentHash = Optional.absent();

  // Volatile because it might be set and read from different threads
  private volatile boolean aborted;

  /**
   * Sets the offset to the default value of <em>0</em>.
   */
  protected CrawlContext() {
    offset = 0;
  }

  protected CrawlContext(int offset) {
    setOffset(offset);
  }

  public int getOffset() {
    return offset;
  }

  /**
   * Sets the offset to a valid positive value (greater than or equal to 0).
   *
   * @param offset the new offset
   */
  public final void setOffset(int offset) {
    Preconditions.checkArgument(offset >= 0, "Offset has to be greater than or equal to 0");

    this.offset = offset;
  }

  /**
   * Returns true if the crawl should be aborted. Once this returns true it can never return false again.
   *
   * @return whether this crawl should be aborted
   */
  public boolean isAborted() {
    return aborted;
  }

  /**
   * This lets us abort a running crawl. Once aborted it can't be undone. There is no guarantee as to how long it takes
   * to actually stop the crawl.
   */
  public void abort() {
    aborted = true;
  }

  /**
   * Indicates whether the last request we made was a speculative one.
   *
   * @return true if the next request to make (corresponding to this context) is a speculative one
   */
  public boolean isSpeculative() {
    return speculative;
  }

  public void setSpeculative(boolean speculative) {
    this.speculative = speculative;
  }

  /**
   * A hash of the last content we received. To be used to detect duplicate content and prevent infinite looping. This
   * is not the hash of the request corresponding to this context but to the last successful one before this.
   */
  public Optional<Long> getLastContentHash() {
    return lastContentHash;
  }

  public void setLastContentHash(Optional<Long> lastContentHash) {
    this.lastContentHash = checkNotNull(lastContentHash, "lastContentHash can't be null");
  }

}
