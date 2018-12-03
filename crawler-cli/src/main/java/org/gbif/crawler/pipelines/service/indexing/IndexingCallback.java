package org.gbif.crawler.pipelines.service.indexing;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.pipelines.config.IndexingConfiguration;
import org.gbif.crawler.pipelines.service.HdfsUtils;
import org.gbif.crawler.pipelines.service.PipelineCallback;
import org.gbif.crawler.pipelines.service.PipelineCallback.Steps;
import org.gbif.crawler.pipelines.service.indexing.ProcessRunnerBuilder.RunnerEnum;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.PipelinesNodePaths.INTERPRETED_TO_INDEX;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 */
public class IndexingCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingCallback.class);
  private final IndexingConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  IndexingCallback(IndexingConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesInterpretedMessage} message
   */
  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    String attempt = Integer.toString(message.getAttempt());
    Set<String> steps = message.getPipelineSteps();

    // Main message processing logic, creates a terminal java process, which runs interpreted-to-index pipeline
    Runnable runnable = () -> {
      try {
        String path = String.join("/", config.repositoryPath, datasetId.toString(), attempt, "interpreted", "basic");

        // Chooses a runner type by calculating number of files
        int filesCount = HdfsUtils.getfileCount(path, config.hdfsSiteConfig);
        RunnerEnum runner = filesCount > config.switchFilesNumber ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
        LOG.info("Number of files - {}, Spark Runner type - {}", filesCount, runner);

        // Number of Spark threads
        config.sparkParallelism = filesCount;

        // Assembles a terminal java process and runs it
        ProcessRunnerBuilder.create()
          .config(config)
          .message(message)
          .runner(runner)
          .esIndexName(datasetId + "_" + attempt)
          .esAlias(datasetId + "," + config.idxAlias)
          .build()
          .start()
          .waitFor();

      } catch (InterruptedException | IOException ex) {
        LOG.error(ex.getMessage(), ex);
        throw new IllegalStateException("Failed performing interpretation on " + datasetId, ex);
      }
    };

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
      .incomingMessage(message)
      .outgoingMessage(new PipelinesIndexedMessage(datasetId, message.getAttempt(), steps))
      .curator(curator)
      .zkRootElementPath(INTERPRETED_TO_INDEX)
      .pipelinesStepName(Steps.INTERPRETED_TO_INDEX.name())
      .publisher(publisher)
      .runnable(runnable)
      .build()
      .handleMessage();
  }

}
