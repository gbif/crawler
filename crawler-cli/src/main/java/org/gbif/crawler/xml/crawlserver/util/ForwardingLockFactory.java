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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ForwardingObject;

public abstract class ForwardingLockFactory extends ForwardingObject implements LockFactory {

  public abstract class ForwardingLock extends ForwardingObject implements Lock {

    protected ForwardingLock() {}

    @Override
    public void lock() {
      delegate().lock();
    }

    @Override
    public boolean tryLock() {
      return delegate().tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      return delegate().tryLock(time, unit);
    }

    @Override
    public void unlock() {
      delegate().unlock();
    }

    @Override
    protected abstract Lock delegate();
  }

  protected ForwardingLockFactory() {}

  @Override
  public void clearLock(String name) {
    delegate().clearLock(name);
  }

  @Override
  public Lock makeLock(String name) {
    return delegate().makeLock(name);
  }

  @Override
  protected abstract LockFactory delegate();
}
