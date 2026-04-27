package org.gbif.crawler.dwcdp.metasync;

import org.gbif.crawler.common.OkHttpRegistryMetadataClient;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;

public class DwcDpMetasyncService extends AbstractIdleService {

  private final DwcDpMetasyncConfiguration config;
  private MessageListener listener;
  private CuratorFramework curator;

  public DwcDpMetasyncService(DwcDpMetasyncConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    curator = config.zooKeeper.getCuratorFramework();

    OkHttpRegistryMetadataClient registryClient = new OkHttpRegistryMetadataClient(config.registry);
    DwcDpMetasyncCallback callback =
        new DwcDpMetasyncCallback(
            registryClient, config.archiveRepository, curator, new DwcDpMetadataDocumentConverter());

    listener.listen(
        config.queueName, DwcDpDownloadFinishedMessage.ROUTING_KEY, config.poolSize, callback);
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
