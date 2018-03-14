package org.gbif.crawler.pipelines.xml;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;

import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for the {@link XmlToAvroCommand}.
 * <p>
 * This service listens to {@link CrawlFinishedMessage}.
 */
public class XmlToAvroService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroService.class);
  private final XmlToAvroConfiguration configuration;
  private MessageListener listener;

  public XmlToAvroService(XmlToAvroConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {

    // create the listener.
    listener = new MessageListener(configuration.messaging.getConnectionParameters(), 1);
    // creates a binding between the queue specified in the configuration and the exchange and routing key specified in
    // CrawlFinishedMessage
    listener.listen(configuration.queueName, configuration.threadCount, new AvroPropagatorCallback());
  }

  @Override
  protected void shutDown() throws Exception {
    listener.close();
  }

  private static class AvroPropagatorCallback extends AbstractMessageCallback<CrawlFinishedMessage> {

    @Override
    public void handleMessage(CrawlFinishedMessage message) {

      LOG.info("CrawlFinsihedMessage received");

      // Add processing here...

    }
  }

}
