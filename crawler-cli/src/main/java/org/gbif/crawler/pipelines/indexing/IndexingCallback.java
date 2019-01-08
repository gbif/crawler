package org.gbif.crawler.pipelines.indexing;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.pipelines.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;
import org.gbif.crawler.pipelines.indexing.ProcessRunnerBuilder.RunnerEnum;

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

    UUID datasetId = message.getDatasetUuid();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

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

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index pipeline
   */
  private Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      try {
        String datasetId = message.getDatasetUuid().toString();
        String attempt = Integer.toString(message.getAttempt());

        // Chooses a runner type by calculating verbatim.avro file size
        String verbatimPath = String.join("/", config.repositoryPath, datasetId, attempt, "verbatim.avro");
        long fileSizeByte = HdfsUtils.getfileSizeByte(verbatimPath, config.hdfsSiteConfig);

        // Chooses a runner type by calculating number of files
        String basicPath = String.join("/", config.repositoryPath, datasetId, attempt, "interpreted", "basic");
        int filesCount = HdfsUtils.getfileCount(basicPath, config.hdfsSiteConfig);

        RunnerEnum runner;

        if (fileSizeByte > 0) {
          long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
          runner = fileSizeByte > switchFileSizeByte ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
          LOG.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);
        } else {
          runner = filesCount > config.switchFilesNumber ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
          LOG.info("Number of files - {}, Spark Runner type - {}", filesCount, runner);
        }

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
        throw new IllegalStateException("Failed indexing on " + message.getDatasetUuid(), ex);
      }
    };
  }

}
