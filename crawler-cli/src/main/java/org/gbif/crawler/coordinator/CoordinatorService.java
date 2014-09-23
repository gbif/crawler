package org.gbif.crawler.coordinator;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.crawler.CrawlerCoordinatorService;
import org.gbif.crawler.CrawlerCoordinatorServiceImpl;
import org.gbif.crawler.StartCrawlMessageCallback;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Injector;
import org.apache.curator.framework.CuratorFramework;

/**
 * This services starts the Crawler Coordinator by listening for messages.
 */
public class CoordinatorService extends AbstractIdleService {

  private final CoordinatorConfiguration configuration;

  private MessageListener listener;

  private CuratorFramework curator;

  public CoordinatorService(CoordinatorConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    // Create ZooKeeper and RabbitMQ connections
    curator = configuration.zooKeeper.getCuratorFramework();

    // Create Registry WS Client
    Injector injector = configuration.registry.newRegistryInjector();
    DatasetService datasetService = injector.getInstance(DatasetService.class);

    CrawlerCoordinatorService coord = new CrawlerCoordinatorServiceImpl(curator, datasetService);
    MessageCallback<StartCrawlMessage> callback = new StartCrawlMessageCallback(coord);

    listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.queueName, configuration.poolSize, callback);
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
    if (curator != null) {
      curator.close();
    }
  }
}
