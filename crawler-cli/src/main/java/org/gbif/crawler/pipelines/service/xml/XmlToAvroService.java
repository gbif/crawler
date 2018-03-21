package org.gbif.crawler.pipelines.service.xml;

import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;
import org.gbif.crawler.pipelines.ConverterConfiguration;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * Service for the {@link XmlToAvroCommand}.
 * <p>
 * This service listens to {@link CrawlFinishedMessage}.
 */
public class XmlToAvroService extends AbstractIdleService {

  private final ConverterConfiguration configuration;
  private MessageListener listener;

  public XmlToAvroService(ConverterConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    // create the listener.
    listener = new MessageListener(configuration.messaging.getConnectionParameters(), 1);
    // creates a binding between the queue specified in the configuration and the exchange and routing key specified in
    // CrawlFinishedMessage
    listener.listen(configuration.queueName, configuration.poolSize, new XmlToAvroCallBack(configuration));
  }

  @Override
  protected void shutDown() {
    listener.close();
  }

}