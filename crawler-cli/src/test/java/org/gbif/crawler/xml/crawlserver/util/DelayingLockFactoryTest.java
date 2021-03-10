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
package org.gbif.crawler.xml.crawlserver.util;

import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.NoLockFactory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertTrue(end > start);
    assertTrue(start + 200 <= end);
  }
}
