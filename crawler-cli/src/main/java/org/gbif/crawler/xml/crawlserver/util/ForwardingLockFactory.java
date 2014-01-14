package org.gbif.crawler.xml.crawlserver.util;

import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ForwardingObject;

public abstract class ForwardingLockFactory extends ForwardingObject implements LockFactory {

  public abstract class ForwardingLock extends ForwardingObject implements Lock {

    protected ForwardingLock() {
    }

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

  protected ForwardingLockFactory() {
  }

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
