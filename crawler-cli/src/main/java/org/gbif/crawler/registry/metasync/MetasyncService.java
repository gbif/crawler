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
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetasyncService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MetasyncService.class);
  private final MetasyncConfiguration configuration;
  private MessageListener listener;

  public MetasyncService(MetasyncConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    Properties props = new Properties();
    props.setProperty("registry.ws.url", configuration.registry.wsUrl);
    Injector injector = Guice.createInjector(new RegistryWsClientModule(props),
      new SingleUserAuthModule(configuration.registry.user, configuration.registry.password));

    DatasetService datasetService = injector.getInstance(DatasetService.class);
    MetasyncHistoryService historyService = injector.getInstance(MetasyncHistoryService.class);
    InstallationService installationService = injector.getInstance(InstallationService.class);

    AbstractMessageCallback<StartMetasyncMessage> metasyncCallback;

    if (configuration.dryRun) {
      LOG.warn("Metasync dry run: the registry will not be updated.");
      metasyncCallback = new DebugMetasyncCallback(installationService);
    } else {
      RegistryUpdater registryUpdater = new RegistryUpdater(datasetService, historyService);
      metasyncCallback = new MetasyncCallback(registryUpdater, installationService);
    }

    listener = new MessageListener(configuration.messaging.getConnectionParameters(), 1);
    listener.listen(configuration.queueName, configuration.threadCount, metasyncCallback);
  }

  @Override
  protected void shutDown() throws Exception {
    listener.close();
  }

  /**
   * Handles the synchronization, but only logs debug information.
   */
  private static class DebugMetasyncCallback extends AbstractMessageCallback<StartMetasyncMessage> {
    private final MetadataSynchroniser synchroniser;

    private DebugMetasyncCallback(InstallationService installationService) {
      HttpClientFactory clientFactory = new HttpClientFactory(120, TimeUnit.MINUTES);

      MetadataSynchroniserImpl newSynchroniser = new MetadataSynchroniserImpl(installationService);

      newSynchroniser.registerProtocolHandler(new DigirMetadataSynchroniser(clientFactory.provideHttpClient()));
      newSynchroniser.registerProtocolHandler(new TapirMetadataSynchroniser(clientFactory.provideHttpClient()));
      newSynchroniser.registerProtocolHandler(new BiocaseMetadataSynchroniser(clientFactory.provideHttpClient()));
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

  /**
   * Extends the debug handler to also update the registry.
   */
  private static class MetasyncCallback extends DebugMetasyncCallback {
    private final RegistryUpdater registryUpdater;

    private MetasyncCallback(RegistryUpdater registryUpdater, InstallationService installationService) {
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
