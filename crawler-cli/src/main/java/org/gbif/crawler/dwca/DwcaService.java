package org.gbif.crawler.dwca;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

/**
 * Basic service that connects to RabbitMQ and binds a writable DatasetService for the GBIF registry.
 * Implement {@link #bindListeners} to listen to any messages or configure anything else on service startup.
 */
public abstract class DwcaService extends AbstractIdleService {

  protected final DwcaConfiguration config;
  protected MessagePublisher publisher;
  protected MessageListener listener;
  protected DatasetService datasetService;

  protected DwcaService(DwcaConfiguration config) {
    this.config = Preconditions.checkNotNull(config, "Configuration required");
  }

  /**
   * Does all the registry client bindings and messaging startup, binding listener via the abstract bindListeners()
   * method.
   */
  @Override
  protected void startUp() throws Exception {
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    listener = new MessageListener(config.messaging.getConnectionParameters());
    datasetService = config.registry.newRegistryInjector().getInstance(DatasetService.class);
    bindListeners();
  }

  @Override
  protected void shutDown() throws Exception {
    publisher.close();
    listener.close();
  }

  /**
   * Implement this to bind any messaging listeners once after the messaging service has been started up.
   */
  protected abstract void bindListeners() throws IOException;

}
