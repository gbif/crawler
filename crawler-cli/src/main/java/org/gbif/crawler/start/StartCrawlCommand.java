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
package org.gbif.crawler.start;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;

import java.io.IOException;
import java.util.UUID;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts a crawl by sending a message (which then needs to be picked up by the Coordinator. */
@MetaInfServices(Command.class)
public class StartCrawlCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(StartCrawlCommand.class);

  private final StartCrawlConfiguration config = new StartCrawlConfiguration();

  public StartCrawlCommand() {
    super("startcrawl");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    try {
      MessagePublisher publisher =
          new DefaultMessagePublisher(config.messaging.getConnectionParameters());
      Message message = new StartCrawlMessage(UUID.fromString(config.uuid), config.priority);
      publisher.send(message);
      LOG.info("Sent message to crawl [{}]", config.uuid);
    } catch (IOException e) {
      LOG.error("Caught exception while sending crawl message", e);
    }
  }
}
