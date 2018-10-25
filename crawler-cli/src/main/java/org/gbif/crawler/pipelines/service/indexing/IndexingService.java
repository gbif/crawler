package org.gbif.crawler.pipelines.service.indexing;

import org.gbif.common.messaging.MessageListener;
import org.gbif.crawler.pipelines.config.IndexingConfiguration;

import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service which listens to the  {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } and perform conversion
 */
public class IndexingService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingService.class);
  private final IndexingConfiguration configuration;
  private MessageListener listener;

  public IndexingService(IndexingConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started index-dataset service with parameters : {}", configuration);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(configuration.messaging.getConnectionParameters(), 1);
    listener.listen(configuration.queueName, configuration.poolSize, new IndexingCallBack(configuration));
  }

  @Override
  protected void shutDown() {
    listener.close();
    LOG.info("Stopping index-dataset service");
  }
}
