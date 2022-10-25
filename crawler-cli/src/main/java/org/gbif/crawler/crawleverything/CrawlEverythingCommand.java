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

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;

/**
 * This command iterates over all datasets from the registry and sends a StartCrawlMessage for each
 * of them.
 */
@SuppressWarnings("UnstableApiUsage")
@MetaInfServices(Command.class)
public class CrawlEverythingCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlEverythingCommand.class);
  private static final int LIMIT = 1000;
  private final EverythingConfiguration config = new EverythingConfiguration();

  public CrawlEverythingCommand() {
    super("crawleverything");
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
      ClientBuilder clientBuilder = new ClientBuilder().withUrl(config.registryWsUrl)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport());

      DatasetService datasetService = clientBuilder.build(DatasetClient.class);

      ExecutorService executor = Executors.newFixedThreadPool(20);

      Random random = new Random();
      AtomicInteger totalCount = new AtomicInteger();
      AtomicInteger scheduledCount = new AtomicInteger();

      PagingRequest request = new PagingRequest(0, LIMIT);
      PagingResponse<Dataset> response = null;
      do {
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOG.info("Requesting batch of datasets starting at offset [{}]", request.getOffset());
        try {
          response = datasetService.list(request);
          stopwatch.stop();
          LOG.info(
              "Received [{}] datasets in [{}]s",
              response.getResults().size(),
              stopwatch.elapsed(TimeUnit.SECONDS));

          // we never reach the limits of integers for dataset numbers. Simple casting is fine
          executor.submit(
              new SchedulingRunnable(
                  response,
                  random,
                  publisher,
                  totalCount,
                  scheduledCount,
                  (int) request.getOffset()));

        } catch (Exception e) {
          LOG.error(
              "Got error requesting datasets, skipping to offset [{}]", request.getOffset(), e);
        }
        // increase offset
        request.nextPage();

      } while (response == null || !response.isEndOfRecords());

      executor.shutdown();
      while (!executor.isTerminated()) {
        try {
          LOG.info("Waiting for completion of scheduling...");
          executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          LOG.error("Waiting for completion interrupted", e);
        }
      }

      publisher.close();
      LOG.info(
          "Done processing [{}] datasets, [{}] were scheduled",
          totalCount.get(),
          scheduledCount.get());
    } catch (IOException e) {
      throw Throwables.propagate(e); // we're hosed
    }
  }

  private static class SchedulingRunnable implements Runnable {

    private final int offset;
    private final PagingResponse<Dataset> datasets;
    private final Random random;
    private final MessagePublisher publisher;
    private final AtomicInteger count;
    private final AtomicInteger scheduledCount;

    private SchedulingRunnable(
        PagingResponse<Dataset> datasets,
        Random random,
        MessagePublisher publisher,
        AtomicInteger count,
        AtomicInteger scheduledCount,
        int offset) {
      this.datasets = datasets;
      this.random = random;
      this.publisher = publisher;
      this.count = count;
      this.offset = offset;
      this.scheduledCount = scheduledCount;
    }

    @Override
    public void run() {
      int registeredCount = 0;
      for (Dataset dataset : datasets.getResults()) {
        count.incrementAndGet();
        if (!dataset.isExternal()) {
          Message message = new StartCrawlMessage(dataset.getKey(), random.nextInt(99) + 1);
          try {
            publisher.send(message);
            registeredCount++;
            scheduledCount.incrementAndGet();
            LOG.debug("Scheduled crawl of [{}]", dataset.getKey());
          } catch (IOException e) {
            LOG.error("Caught exception while sending crawl message", e);
          }
        }
      }
      LOG.debug(
          "[{}] out of [{}] for offset [{}] were registered and scheduled",
          registeredCount,
          datasets.getResults().size(),
          offset);
    }
  }
}
