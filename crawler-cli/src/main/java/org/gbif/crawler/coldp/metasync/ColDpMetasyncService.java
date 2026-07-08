/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.coldp.metasync;

import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.crawler.common.OkHttpRegistryMetadataClient;

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
