package org.gbif.crawler.xml.propagater;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;

import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroPropagatorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AvroPropagatorService.class);
  private final AvroPropagatorConfiguration configuration;
  private MessageListener listener;

  public AvroPropagatorService(AvroPropagatorConfiguration configuration) {
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

      LOG.info("CrawlFinsihedMessage recevied");

      // Add processing here...

    }
  }

}
