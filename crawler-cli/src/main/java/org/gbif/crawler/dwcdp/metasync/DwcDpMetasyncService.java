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
package org.gbif.crawler.dwcdp.metasync;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpValidationFinishedMessage;
import org.gbif.crawler.common.OkHttpRegistryMetadataClient;

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
