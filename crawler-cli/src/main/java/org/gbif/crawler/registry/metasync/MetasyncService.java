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
    RegistryUpdater registryUpdater = new RegistryUpdater(datasetService, historyService);
    InstallationService installationService = injector.getInstance(InstallationService.class);

    listener = new MessageListener(configuration.messaging.getConnectionParameters(), 1);
    listener.listen(configuration.queueName, configuration.threadCount,
      new MetasyncCallback(registryUpdater, installationService));
  }

  @Override
  protected void shutDown() throws Exception {
    listener.close();
  }

  private static class MetasyncCallback extends AbstractMessageCallback<StartMetasyncMessage> {

    private final RegistryUpdater registryUpdater;
    private final MetadataSynchroniser synchroniser;

    private MetasyncCallback(RegistryUpdater registryUpdater, InstallationService installationService) {
      this.registryUpdater = registryUpdater;

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
      DebugHandler.processResult(syncResult);

      registryUpdater.saveSyncResultsToRegistry(ImmutableList.of(syncResult));

    }

  }

}
