/*
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lock factory that forwards all colls to another Lock Factory but delays unlocking for a
 * configurable amount of time. This can be used to make sure that we don't tax the resources of
 * providers too much.
 */
public class DelayingLockFactory extends ForwardingLockFactory {

  /**
   * Default value of milliseconds that unlock operations are delayed if no other value is given on
   * construction.
   */
  public static final long DEFAULT_DELAY = 2000;

  private static final Logger LOG = LoggerFactory.getLogger(DelayingLockFactory.class);

  private final LockFactory delegateFactory;

  private final long delay;

  /**
   * Builds a delaying lock factory that delays every unlock operation by a certain amount of time
   * as specified in the default setting {@link DelayingLockFactory#DEFAULT_DELAY}.
   *
   * @param delegateFactory to forward calls to
   */
  public DelayingLockFactory(LockFactory delegateFactory) {
    this(delegateFactory, DEFAULT_DELAY);
  }

  /**
   * Builds a delaying lock factory that delays every unlock operation by a user-specified amount of
   * milliseconds.
   *
   * @param delegateFactory to forward calls to
   * @param delay in milliseconds
   */
  public DelayingLockFactory(LockFactory delegateFactory, long delay) {
    this.delegateFactory = delegateFactory;
    this.delay = delay;
  }

  @Override
  public Lock makeLock(String name) {
    return new DelayingLock(delegate().makeLock(name), delay);
  }

  @Override
  protected LockFactory delegate() {
    return delegateFactory;
  }

  private class DelayingLock extends ForwardingLock {

    private final Lock delegateLock;

    private DelayingLock(Lock delegateLock, long delay) {
      this.delegateLock = delegateLock;
    }

    @Override
    public void unlock() {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while sleeping");
      }
      delegate().unlock();
    }

    @Override
    protected Lock delegate() {
      return delegateLock;
    }
  }
}
