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
package org.gbif.crawler.registry.metasync;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.MetasyncHistoryService;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.StartMetasyncMessage;
import org.gbif.crawler.metasync.MetadataSynchronizerImpl;
import org.gbif.crawler.metasync.api.MetadataSynchronizer;
import org.gbif.crawler.metasync.api.SyncResult;
import org.gbif.crawler.metasync.protocols.biocase.BiocaseMetadataSynchronizer;
import org.gbif.crawler.metasync.protocols.digir.DigirMetadataSynchronizer;
import org.gbif.crawler.metasync.protocols.tapir.TapirMetadataSynchronizer;
import org.gbif.crawler.metasync.resulthandler.DebugHandler;
import org.gbif.crawler.metasync.resulthandler.RegistryUpdater;
import org.gbif.crawler.metasync.util.HttpClientFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;

@SuppressWarnings("UnstableApiUsage")
public class MetasyncService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MetasyncService.class);
  private final MetasyncConfiguration configuration;
  private MessageListener listener;

  public MetasyncService(MetasyncConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {

    ClientBuilder clientBuilder = new ClientBuilder().withUrl(configuration.registry.wsUrl)
      .withCredentials(configuration.registry.user, configuration.registry.password)
      .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport());

    DatasetService datasetService = clientBuilder.build(DatasetClient.class);
    InstallationService installationService = clientBuilder.build(InstallationClient.class);

    AbstractMessageCallback<StartMetasyncMessage> metasyncCallback;

    if (configuration.dryRun) {
      LOG.warn("Metasync dry run: the registry will not be updated.");
      metasyncCallback = new DebugMetasyncCallback(installationService);
    } else {
      RegistryUpdater registryUpdater =
          new RegistryUpdater(datasetService, ((MetasyncHistoryService) installationService));
      metasyncCallback = new MetasyncCallback(registryUpdater, installationService);
    }

    listener = new MessageListener(configuration.messaging.getConnectionParameters(), 1);
    listener.listen(configuration.queueName, configuration.threadCount, metasyncCallback);
  }

  @Override
  protected void shutDown() {
    listener.close();
  }

  /** Handles the synchronization, but only logs debug information. */
  private static class DebugMetasyncCallback extends AbstractMessageCallback<StartMetasyncMessage> {
    private final MetadataSynchronizer Synchronizer;

    private DebugMetasyncCallback(InstallationService installationService) {
      HttpClientFactory clientFactory = new HttpClientFactory(120, TimeUnit.MINUTES);

      MetadataSynchronizerImpl newSynchronizer = new MetadataSynchronizerImpl(installationService);

      newSynchronizer.registerProtocolHandler(
          new DigirMetadataSynchronizer(clientFactory.provideHttpClient()));
      newSynchronizer.registerProtocolHandler(
          new TapirMetadataSynchronizer(clientFactory.provideHttpClient()));
      newSynchronizer.registerProtocolHandler(
          new BiocaseMetadataSynchronizer(clientFactory.provideHttpClient()));
      this.Synchronizer = newSynchronizer;
    }

    @Override
    public void handleMessage(StartMetasyncMessage message) {
      SyncResult syncResult = Synchronizer.synchronizeInstallation(message.getInstallationKey());
      LOG.info("Done syncing. Processing result.");
      handleSyncResult(syncResult);
    }

    void handleSyncResult(SyncResult syncResult) {
      DebugHandler.processResult(syncResult);
    }
  }

  /** Extends the debug handler to also update the registry. */
  private static class MetasyncCallback extends DebugMetasyncCallback {
    private final RegistryUpdater registryUpdater;

    private MetasyncCallback(
        RegistryUpdater registryUpdater, InstallationService installationService) {
      super(installationService);
      this.registryUpdater = registryUpdater;
    }

    @Override
    void handleSyncResult(SyncResult syncResult) {
      super.handleSyncResult(syncResult);

      registryUpdater.saveSyncResultsToRegistry(ImmutableList.of(syncResult));
    }
  }
}
