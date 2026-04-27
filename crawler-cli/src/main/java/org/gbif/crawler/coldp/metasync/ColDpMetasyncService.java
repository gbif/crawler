package org.gbif.crawler.coldp.metasync;

import org.gbif.crawler.common.OkHttpRegistryMetadataClient;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;

public class ColDpMetasyncService extends AbstractIdleService {

  private final ColDpMetasyncConfiguration config;
  private MessageListener listener;
  private CuratorFramework curator;

  public ColDpMetasyncService(ColDpMetasyncConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    curator = config.zooKeeper.getCuratorFramework();

    OkHttpRegistryMetadataClient registryClient = new OkHttpRegistryMetadataClient(config.registry);
    ColDpMetasyncCallback callback =
        new ColDpMetasyncCallback(
            registryClient, config.archiveRepository, curator, new ColDpMetadataDocumentConverter());

    listener.listen(
        config.queueName, ColDpDownloadFinishedMessage.ROUTING_KEY, config.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    if (listener != null) {
      listener.close();
    }
    if (curator != null) {
      curator.close();
    }
  }
}
