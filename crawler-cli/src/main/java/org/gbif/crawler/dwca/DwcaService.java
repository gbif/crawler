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
package org.gbif.crawler.dwca;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.registry.ws.client.DatasetClient;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

/**
 * Basic service that connects to RabbitMQ and binds a writable DatasetService for the GBIF
 * registry. Implement {@link #bindListeners} to listen to any messages or configure anything else
 * on service startup.
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
   * Does all the registry client bindings and messaging startup, binding listener via the abstract
   * bindListeners() method.
   */
  @Override
  protected void startUp() throws Exception {
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    // dwca messages are rather long-running processes. Only prefetch one message at a time
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    datasetService = config.registry.newClientBuilder().build(DatasetClient.class);
    bindListeners();
  }

  @Override
  protected void shutDown() throws Exception {
    publisher.close();
    listener.close();
  }

  /**
   * Implement this to bind any messaging listeners once after the messaging service has been
   * started up.
   */
  protected abstract void bindListeners() throws IOException;
}
