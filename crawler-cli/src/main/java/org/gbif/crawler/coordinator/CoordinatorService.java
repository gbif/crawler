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
package org.gbif.crawler.coordinator;

import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.crawler.CrawlerCoordinatorService;
import org.gbif.crawler.CrawlerCoordinatorServiceImpl;
import org.gbif.crawler.StartCrawlMessageCallback;
import org.gbif.crawler.metasync.MetadataSynchronizerImpl;
import org.gbif.crawler.metasync.protocols.biocase.BiocaseMetadataSynchronizer;
import org.gbif.crawler.metasync.protocols.digir.DigirMetadataSynchronizer;
import org.gbif.crawler.metasync.protocols.tapir.TapirMetadataSynchronizer;
import org.gbif.crawler.metasync.util.HttpClientFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.DatasetProcessStatusClient;
import org.gbif.registry.ws.client.InstallationClient;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;

/** This services starts the Crawler Coordinator by listening for messages. */
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
    DatasetService datasetService = configuration.registry.newClientBuilder().build(DatasetClient.class);
    DatasetProcessStatusService datasetProcessStatusService = configuration.registry.newClientBuilder().build(DatasetProcessStatusClient.class);
    InstallationService installationService = configuration.registry.newClientBuilder().build(InstallationClient.class);

    HttpClientFactory clientFactory = new HttpClientFactory(30, TimeUnit.SECONDS);
    MetadataSynchronizerImpl metadataSynchronizer =
        new MetadataSynchronizerImpl(installationService);
    metadataSynchronizer.registerProtocolHandler(
        new DigirMetadataSynchronizer(clientFactory.provideHttpClient()));
    metadataSynchronizer.registerProtocolHandler(
        new TapirMetadataSynchronizer(clientFactory.provideHttpClient()));
    metadataSynchronizer.registerProtocolHandler(
        new BiocaseMetadataSynchronizer(clientFactory.provideHttpClient()));

    CrawlerCoordinatorService coord =
        new CrawlerCoordinatorServiceImpl(curator, datasetService, datasetProcessStatusService, metadataSynchronizer);
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
