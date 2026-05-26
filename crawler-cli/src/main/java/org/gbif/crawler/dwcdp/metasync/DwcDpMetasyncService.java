package org.gbif.crawler.dwcdp.metasync;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpValidationFinishedMessage;
import org.gbif.crawler.common.OkHttpRegistryMetadataClient;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;

public class DwcDpMetasyncService extends AbstractIdleService {

  private final DwcDpMetasyncConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public DwcDpMetasyncService(DwcDpMetasyncConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();

    OkHttpRegistryMetadataClient registryClient = new OkHttpRegistryMetadataClient(config.registry);
    DwcDpMetasyncCallback callback =
        new DwcDpMetasyncCallback(
            registryClient,
            config.archiveRepository,
            curator,
            publisher,
            new DwcDpMetadataDocumentConverter());

    listener.listen(
        config.queueName, DwcDpValidationFinishedMessage.ROUTING_KEY, config.poolSize, callback);
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
    if (publisher != null) {
      publisher.close();
    }
    if (curator != null) {
      curator.close();
    }
  }
}
