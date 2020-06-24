/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.crawler.CrawlerCoordinatorService;
import org.gbif.crawler.CrawlerCoordinatorServiceImpl;
import org.gbif.crawler.StartCrawlMessageCallback;
import org.gbif.registry.metasync.MetadataSynchroniserImpl;
import org.gbif.registry.metasync.protocols.biocase.BiocaseMetadataSynchroniser;
import org.gbif.registry.metasync.protocols.digir.DigirMetadataSynchroniser;
import org.gbif.registry.metasync.protocols.tapir.TapirMetadataSynchroniser;
import org.gbif.registry.metasync.util.HttpClientFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.ws.client.ClientFactory;

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
    ClientFactory wsClientFactory = configuration.registry.newClientFactory();
    DatasetService datasetService = wsClientFactory.newInstance(DatasetClient.class);
    InstallationService installationService = wsClientFactory.newInstance(InstallationClient.class);

    HttpClientFactory clientFactory = new HttpClientFactory(30, TimeUnit.SECONDS);
    MetadataSynchroniserImpl metadataSynchroniser =
        new MetadataSynchroniserImpl(installationService);
    metadataSynchroniser.registerProtocolHandler(
        new DigirMetadataSynchroniser(clientFactory.provideHttpClient()));
    metadataSynchroniser.registerProtocolHandler(
        new TapirMetadataSynchroniser(clientFactory.provideHttpClient()));
    metadataSynchroniser.registerProtocolHandler(
        new BiocaseMetadataSynchroniser(clientFactory.provideHttpClient()));

    CrawlerCoordinatorService coord =
        new CrawlerCoordinatorServiceImpl(curator, datasetService, metadataSynchroniser);
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
