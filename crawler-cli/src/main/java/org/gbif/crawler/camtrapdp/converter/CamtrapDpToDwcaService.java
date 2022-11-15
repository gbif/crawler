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
package org.gbif.crawler.camtrapdp.converter;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.camtrapdp.CamtrapDpConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;

import lombok.extern.slf4j.Slf4j;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.CamtrapDpDownloadFinishedMessage} and performs a conversion from
 * CamtrapDP to DwC-A.
 */
@Slf4j
public class CamtrapDpToDwcaService extends AbstractIdleService {

  private final CamtrapDpConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public CamtrapDpToDwcaService(CamtrapDpConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started crawler-camtrapdp-to-dwca service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();

    DatasetClient datasetClient =
        new ClientBuilder()
            .withUrl(config.registry.wsUrl)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .build(DatasetClient.class);


    CamtrapDpToDwcaCallback callback =
        new CamtrapDpToDwcaCallback(config, curator, publisher, datasetClient);


    listener.listen(config.queueName, callback.getRouting(), config.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    log.info("Stopping crawler-camtrapdp-to-dwca service");
  }
}
