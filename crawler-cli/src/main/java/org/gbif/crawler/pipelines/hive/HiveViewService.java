package org.gbif.crawler.pipelines.hive;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage } and perform conversion
 */
public class HiveViewService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(HiveViewService.class);
  private final HiveViewConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public HiveViewService(HiveViewConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started hive-view service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();

    listener.listen(config.queueName, config.poolSize, new HiveViewCallback(config, publisher, curator));
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    LOG.info("Stopping hive-view service");
  }
}
