package org.gbif.crawler.pipelines.indexing;

import java.io.IOException;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;

import org.apache.curator.framework.CuratorFramework;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * A service which listens to the  {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
public class IndexingService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingService.class);
  private final IndexingConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;
  private CloseableHttpClient httpClient;
  private DatasetService datasetService;


  public IndexingService(IndexingConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started pipelines-index-dataset service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();
    datasetService = config.registry.newRegistryInjector().getInstance(DatasetService.class);
    httpClient = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(60_000)
            .setSocketTimeout(60_000)
            .build())
        .build();

    listener.listen(config.queueName, config.poolSize, new IndexingCallback(config, publisher, datasetService, curator, httpClient));
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    try {
      httpClient.close();
    } catch (IOException e) {
      LOG.error("Can't close ES http client connection");
    }
    LOG.info("Stopping pipelines-index-dataset service");
  }
}
