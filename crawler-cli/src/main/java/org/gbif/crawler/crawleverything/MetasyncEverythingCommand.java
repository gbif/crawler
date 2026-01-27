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
package org.gbif.crawler.crawleverything;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.StartMetasyncMessage;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

/**
 * This command iterates over all installations from the registry and sends a StartMetasyncMessage
 * for each of them.
 */
@SuppressWarnings("UnstableApiUsage")
@MetaInfServices(Command.class)
public class MetasyncEverythingCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(MetasyncEverythingCommand.class);
  private static final int LIMIT = 1000;
  private final EverythingConfiguration config = new EverythingConfiguration();

  public MetasyncEverythingCommand() {
    super("metasynceverything");
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

      // Create Registry WS Client
      ClientBuilder clientBuilder = new ClientBuilder().withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport());
      InstallationService installationService = clientBuilder.withUrl(config.registryWsUrl).build(InstallationClient.class);

      int offset = 0;
      boolean endOfRecords = true;
      ExecutorService executor = Executors.newFixedThreadPool(20);
      AtomicInteger totalCount = new AtomicInteger();
      AtomicInteger scheduledCount = new AtomicInteger();
      do {
        Pageable request = new PagingRequest(offset, LIMIT);
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOG.info("Requesting batch of installations starting at offset [{}]", request.getOffset());
        PagingResponse<Installation> installations;
        try {
          installations = installationService.list(request);
        } catch (Exception e) {
          offset += LIMIT;
          LOG.error("Got error requesting installations, skipping to offset [{}]", offset, e);
          continue;
        }
        stopwatch.stop();
        LOG.info(
            "Received [{}] installations in [{}]s",
            installations.getResults().size(),
            stopwatch.elapsed(TimeUnit.SECONDS));

        // skip installations that don't serve any datasets!
        Iterator<Installation> iter = installations.getResults().iterator();
        while (iter.hasNext()) {
          Installation i = iter.next();
          PagingResponse<Dataset> datasets =
              installationService.getHostedDatasets(i.getKey(), new PagingRequest(0, 1));
          if (datasets.getResults().isEmpty()) {
            LOG.warn("Excluding installation [key={}] because it serves 0 datasets!", i.getKey());
            iter.remove();
          }
        }

        executor.submit(
            new InstallationSchedulingRunnable(
                installations, publisher, totalCount, scheduledCount, offset));

        endOfRecords = installations.isEndOfRecords();
        offset += installations.getResults().size();
      } while (!endOfRecords);

      executor.shutdown();
      while (!executor.isTerminated()) {
        try {
          LOG.info("Waiting for completion of metasync...");
          executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          LOG.error("Waiting for completion interrupted", e);
        }
      }
      publisher.close();
      LOG.info(
          "Done processing [{}] installations, [{}] were scheduled",
          totalCount.get(),
          scheduledCount.get());
    } catch (IOException e) {
      throw new RuntimeException(e); // we're hosed
    }
  }

  private static class InstallationSchedulingRunnable implements Runnable {

    private final int offset;
    private final PagingResponse<Installation> installations;
    private final MessagePublisher publisher;
    private final AtomicInteger count;
    private final AtomicInteger scheduledCount;

    private InstallationSchedulingRunnable(
        PagingResponse<Installation> installations,
        MessagePublisher publisher,
        AtomicInteger count,
        AtomicInteger scheduledCount,
        int offset) {
      this.installations = installations;
      this.publisher = publisher;
      this.count = count;
      this.offset = offset;
      this.scheduledCount = scheduledCount;
    }

    @Override
    public void run() {
      int registeredCount = 0;
      for (Installation installation : installations.getResults()) {
        count.incrementAndGet();
        Message message = new StartMetasyncMessage(installation.getKey());
        try {
          publisher.send(message);
          registeredCount++;
          scheduledCount.incrementAndGet();
          LOG.debug("Scheduled metasync of installation [{}]", installation.getKey());
        } catch (IOException e) {
          LOG.error("Caught exception while sending metasync message", e);
        }
      }
      LOG.debug(
          "[{}] installations out of [{}] for offset [{}] were registered and scheduled",
          registeredCount,
          installations.getResults().size(),
          offset);
    }
  }
}
