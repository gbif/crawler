package org.gbif.crawler.pipelines.hdfs;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
public class HdfsViewService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsViewService.class);
  private final HdfsViewConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public HdfsViewService(HdfsViewConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started pipelines-hdfs-view service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();
    PipelinesHistoryWsClient historyWsClient =
      config.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    listener.listen(config.queueName, config.poolSize, new HdfsViewCallback(config, publisher, curator, historyWsClient));
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    LOG.info("Stopping pipelines-hdfs-view service");
  }
}
