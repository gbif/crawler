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
package org.gbif.crawler.common.crawlserver;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
import org.apache.curator.framework.recipes.queue.ErrorMode;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

/**
 * A crawl server base class that listens to a zookeeper queue for requested crawls and delegates
 * these requests to a crawl consumer instance which is created once on startup through {@link
 * #newConsumer(CuratorFramework, MessagePublisher, CrawlServerConfiguration)} which needs to be
 * implemented. The zookeeper crawl queuing and locking queue names are passed into the constructor
 * together with further configurations.
 */
public abstract class CrawlServerBaseService<T extends CrawlServerConfiguration>
    extends AbstractIdleService {

  private final String queuedCrawls;
  private final String runningCrawls;

  private final T config;

  private CuratorFramework curator;
  private MessagePublisher publisher;
  private final Set<DistributedPriorityQueue<UUID>> queues = Sets.newHashSet();

  /**
   * @param queuedCrawls zookeeper queue name to watch for new crawls
   * @param runningCrawls zookeeper queue name to lock newly started crawls
   */
  protected CrawlServerBaseService(String queuedCrawls, String runningCrawls, T config) {
    this.queuedCrawls = queuedCrawls;
    this.runningCrawls = runningCrawls;
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());

    curator = config.zooKeeper.getCuratorFramework();
    QueueConsumer<UUID> consumer = newConsumer(curator, publisher, config);
    UuidSerializer serializer = new UuidSerializer();

    // Here we are starting as many queue listeners as specified in the configuration, each will
    // process one queue
    // item at a time but they will all call the same QueueConsumer instance for this
    for (int i = 0; i < config.poolSize; i++) {
      QueueBuilder<UUID> builder =
          QueueBuilder.builder(curator, consumer, serializer, queuedCrawls);
      DistributedPriorityQueue<UUID> queue = builder.lockPath(runningCrawls).buildPriorityQueue(1);
      queue.setErrorMode(ErrorMode.DELETE);
      queues.add(queue);
      queue.start();
    }
  }

  /**
   * Called once on startup to generate a consumer for new crawls. The single consumer instance
   * needs to be thread-safe.
   *
   * @return the crawl consumer for this zookeeper queue
   */
  protected abstract QueueConsumer<UUID> newConsumer(
      CuratorFramework curator, MessagePublisher publisher, T config);

  @Override
  protected void shutDown() throws Exception {
    publisher.close();

    for (DistributedPriorityQueue<UUID> queue : queues) {
      try {
        queue.close();
      } catch (IOException ignored) {
        // just try to close as many as possible
      }
    }

    if (curator != null && curator.getState() == CuratorFrameworkState.STARTED) {
      curator.close();
    }
  }

  /** Needed by the Curator Queue to serialize and deserialize UUIDs */
  private static class UuidSerializer implements QueueSerializer<UUID> {

    @Override
    public byte[] serialize(UUID item) {
      return item.toString().getBytes(Charsets.UTF_8);
    }

    @Override
    public UUID deserialize(byte[] bytes) {
      return UUID.fromString(new String(bytes, Charsets.UTF_8));
    }
  }
}
