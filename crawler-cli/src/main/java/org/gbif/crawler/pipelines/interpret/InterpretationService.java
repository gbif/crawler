package org.gbif.crawler.pipelines.interpret;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service which listens to the  {@link org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage } and perform interpretation
 */
public class InterpretationService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationService.class);

  private final InterpreterConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public InterpretationService(InterpreterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started interpret-dataset service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();

    listener.listen(config.queueName, config.poolSize, new InterpretationCallback(config, publisher, curator));
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    LOG.info("Stopping interpret-dataset service");
  }

}
