package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.crawler.pipelines.config.InterpreterConfiguration;
import org.gbif.crawler.pipelines.service.HdfsUtils;
import org.gbif.crawler.pipelines.service.PipelineCallback;
import org.gbif.crawler.pipelines.service.interpret.ProcessRunnerBuilder.RunnerEnum;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.PipelinesNodePaths.VERBATIM_TO_INTERPRETED;
import static org.gbif.crawler.pipelines.service.PipelineCallback.Steps.INTERPRETED_TO_INDEX;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesVerbatimMessage} is received.
 */
public class InterpretationCallback extends AbstractMessageCallback<PipelinesVerbatimMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationCallback.class);
  private final InterpreterConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  InterpretationCallback(InterpreterConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesVerbatimMessage} message
   */
  @Override
  public void handleMessage(PipelinesVerbatimMessage message) {

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    String attempt = Integer.toString(message.getAttempt());
    Set<String> steps = message.getPipelineSteps();

    // Main message processing logic, creates a terminal java process, which runs verbatim-to-interpreted pipeline
    Runnable runnable = () -> {
      try {
        String path = String.join("/", config.repositoryPath, datasetId.toString(), attempt, "verbatim.avro");

        // Chooses a runner type by calculating file size
        long fileSizeByte = HdfsUtils.getfileSizeByte(path, config.hdfsSiteConfig);
        long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
        RunnerEnum runner = fileSizeByte > switchFileSizeByte ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
        LOG.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);

        // Number of Spark threads
        int numberOfTreads = (int) Math.ceil(fileSizeByte / (config.threadPerMb * 1024d * 1024d));
        config.sparkParallelism = numberOfTreads > 1 ? numberOfTreads : 1;

        LOG.info("Start the process. DatasetId - {}, InterpretTypes - {}, Runner type - {}", datasetId,
          message.getInterpretTypes(), runner);

        // Assembles a terminal java process and runs it
        ProcessRunnerBuilder.create()
          .runner(runner)
          .config(config)
          .message(message)
          .inputPath(path)
          .build()
          .start()
          .waitFor();

      } catch (InterruptedException | IOException ex) {
        LOG.error(ex.getMessage(), ex);
        throw new IllegalStateException("Failed performing interpretation on " + datasetId.toString(), ex);
      }
    };

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
      .incomingMessage(message)
      .outgoingMessage(new PipelinesInterpretedMessage(datasetId, message.getAttempt(), steps))
      .curator(curator)
      .zkRootElementPath(VERBATIM_TO_INTERPRETED)
      .nextPipelinesStep(INTERPRETED_TO_INDEX.name())
      .publisher(publisher)
      .runnable(runnable)
      .build()
      .handleMessage();

  }

}
