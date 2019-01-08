package org.gbif.crawler.pipelines.xml;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for the {@link XmlToAvroCommand}.
 * <p>
 * This service listens to {@link org.gbif.common.messaging.api.messages.PipelinesXmlMessage}.
 */
public class XmlToAvroService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroService.class);

  private final XmlToAvroConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public XmlToAvroService(XmlToAvroConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    // create the listener.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    // creates a binding between the queue specified in the configuration and the exchange and routing key specified in
    // CrawlFinishedMessage
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();

    listener.listen(config.queueName, config.poolSize, new XmlToAvroCallback(config, publisher, curator));
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    LOG.info("Stopping xml-to-avro service");
  }

}
