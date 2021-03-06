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
package org.gbif.crawler.registry.metasync;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.MetasyncHistoryService;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.StartMetasyncMessage;
import org.gbif.registry.metasync.MetadataSynchroniserImpl;
import org.gbif.registry.metasync.api.MetadataSynchroniser;
import org.gbif.registry.metasync.api.SyncResult;
import org.gbif.registry.metasync.protocols.biocase.BiocaseMetadataSynchroniser;
import org.gbif.registry.metasync.protocols.digir.DigirMetadataSynchroniser;
import org.gbif.registry.metasync.protocols.tapir.TapirMetadataSynchroniser;
import org.gbif.registry.metasync.resulthandler.DebugHandler;
import org.gbif.registry.metasync.resulthandler.RegistryUpdater;
import org.gbif.registry.metasync.util.HttpClientFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.ws.client.ClientFactory;

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
    ClientFactory clientFactory =
        new ClientFactory(
            configuration.registry.user,
            configuration.registry.password,
            configuration.registry.wsUrl);

    DatasetService datasetService = clientFactory.newInstance(DatasetClient.class);
    InstallationService installationService = clientFactory.newInstance(InstallationClient.class);

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
    private final MetadataSynchroniser synchroniser;

    private DebugMetasyncCallback(InstallationService installationService) {
      HttpClientFactory clientFactory = new HttpClientFactory(120, TimeUnit.MINUTES);

      MetadataSynchroniserImpl newSynchroniser = new MetadataSynchroniserImpl(installationService);

      newSynchroniser.registerProtocolHandler(
          new DigirMetadataSynchroniser(clientFactory.provideHttpClient()));
      newSynchroniser.registerProtocolHandler(
          new TapirMetadataSynchroniser(clientFactory.provideHttpClient()));
      newSynchroniser.registerProtocolHandler(
          new BiocaseMetadataSynchroniser(clientFactory.provideHttpClient()));
      this.synchroniser = newSynchroniser;
    }

    @Override
    public void handleMessage(StartMetasyncMessage message) {
      SyncResult syncResult = synchroniser.synchroniseInstallation(message.getInstallationKey());
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
