package org.gbif.crawler.xml.crawlserver.util;

import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.NoLockFactory;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class DelayingLockFactoryTest {

  /*
   * This test is not ideal because it depends on actual passing of time etc. The Guava library uses the concept of
   * {@code Ticker} for this very reason but it seems overkill for our usage.
   */
  @Test
  public void testDelay() {
    LockFactory lockFactory = new DelayingLockFactory(new NoLockFactory(), 2000);
    Lock lock = lockFactory.makeLock("foo");
    long start = System.currentTimeMillis();
    lock.lock();
    lock.unlock();
    long end = System.currentTimeMillis();
    assertThat(end).isGreaterThan(start);
    assertThat(start + 2000).isLessThanOrEqualTo(end);
  }

}
