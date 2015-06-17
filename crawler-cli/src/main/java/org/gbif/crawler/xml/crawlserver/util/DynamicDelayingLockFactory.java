/*
 * Copyright ${today.currentYear} Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.xml.crawlserver.util;

import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A lock factory that forwards all calls to another Lock Factory but delays unlocking for a certain amount of time.
 * This can be used to make sure that we don't tax the resources of providers too much. The delay depends on three
 * factors but by default it is the amount of time that the lock was taken out for. So if we held the lock for one
 * second then we're going to delay unlocking for another second. A minimum (default = 100ms and maximum (default = 2s)
 * delay must be specified and we're never going to delay less or more than those bounds.
 */
public class DynamicDelayingLockFactory extends ForwardingLockFactory {

  private class DynamicDelayingLock extends ForwardingLock {

    private final Lock delegateLock;

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    private DynamicDelayingLock(Lock delegateLock) {
      this.delegateLock = delegateLock;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      stopwatch.reset();
      stopwatch.start();
      return delegate().tryLock(time, unit);
    }

    @Override
    public void unlock() {
      stopwatch.stop();

      // Make sure that we delay for a period of time between the given min and maximum delay
      long localDelay = stopwatch.elapsed(MILLISECONDS) > maxDelay ? maxDelay : stopwatch.elapsed(MILLISECONDS);
      localDelay = localDelay < minDelay ? minDelay : localDelay;

      if (localDelay > 0) {
        try {
          Thread.sleep(localDelay);
        } catch (InterruptedException ignored) {
          LOG.warn("Interrupted while sleeping");
        }
      }

      delegate().unlock();
    }

    @Override
    protected Lock delegate() {
      return delegateLock;
    }

  }

  /**
   * Default maximum value of milliseconds that unlock operations are delayed if no other value is given.
   */
  public static final long DEFAULT_MAX_DELAY = 2000;

  /**
   * Default minimum value of milliseconds that unlock operations are delayed if no other value is given.
   */
  private static final int DEFAULT_MIN_DELAY = 100;

  private static final Logger LOG = LoggerFactory.getLogger(DynamicDelayingLockFactory.class);

  private final LockFactory delegateFactory;
  private final long maxDelay;

  private final long minDelay;

  /**
   * Builds a delaying lock factory that delays every unlock operation by a certain amount of time. The exact delay
   * depends on three factors: The time the lock was taken out (i.e. time between acquiring the lock and releasing it),
   * but it will always be between the provided minimum and maximum delay.
   *
   * @param delegateFactory to forward calls to
   */
  public DynamicDelayingLockFactory(LockFactory delegateFactory) {
    this(delegateFactory, DEFAULT_MAX_DELAY, DEFAULT_MIN_DELAY);
  }

  /**
   * Builds a delaying lock factory that delays every unlock operation by a user-specified amount of milliseconds.
   *
   * @param delegateFactory to forward calls to
   * @param maxDelay        in milliseconds
   */
  public DynamicDelayingLockFactory(LockFactory delegateFactory, long minDelay, long maxDelay) {
    this.delegateFactory = checkNotNull(delegateFactory, "delegateFactory can't be null");
    checkArgument(minDelay >= 0, "minDelay must be greater than or equal to 0");
    checkArgument(maxDelay >= 0, "maxDelay must be greater than or equal to 0");
    checkArgument(maxDelay > minDelay, "maxDelay must be greater than minDelay");
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
  }

  @Override
  public Lock makeLock(String name) {
    return new DynamicDelayingLock(delegate().makeLock(name));
  }

  @Override
  protected LockFactory delegate() {
    return delegateFactory;
  }

}
