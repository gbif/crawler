package org.gbif.crawler.dwca.avroconverter;

import org.gbif.common.messaging.MessageListener;

import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service which listens to the  {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } and perform conversion
 */
public class DwCAToAvroService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(DwCAToAvroService.class);
  private final DwCAToAvroConfiguration configuration;
  private MessageListener listener;

  public DwCAToAvroService(DwCAToAvroConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started dwca-to-avro service with parameters : {}", configuration.toString());
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(configuration.messaging.getConnectionParameters(), 1);
    listener.listen(configuration.queueName, configuration.parallelism, new DwCAToAvroConverterCallBack());
  }

  @Override
  protected void shutDown() throws Exception {
    listener.close();
    LOG.info("Stopping dwca-to-avro service");
  }
}
