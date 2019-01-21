package org.gbif.crawler.pipelines.interpret;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.crawler.pipelines.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import static org.gbif.crawler.constants.PipelinesNodePaths.VERBATIM_TO_INTERPRETED;
import static org.gbif.crawler.pipelines.PipelineCallback.Steps.ALL;

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

    if (!isMessageCorrect(message)) {
      return;
    }

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesInterpretedMessage(datasetId, message.getAttempt(), steps, null))
        .curator(curator)
        .zkRootElementPath(VERBATIM_TO_INTERPRETED)
        .pipelinesStepName(Steps.VERBATIM_TO_INTERPRETED.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();

  }

  /** TODO:DOC */
  private boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message.toString());
    }
    return config.processRunner.equals(message.getRunner());
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs verbatim-to-interpreted pipeline
   */
  private Runnable createRunnable(PipelinesVerbatimMessage message) {
    return () -> {
      try {
        String datasetId = message.getDatasetUuid().toString();
        String attempt = Integer.toString(message.getAttempt());

        deleteInterpretationsIfExist(message);

        long recordsNumber = getRecordNumber(message);

        String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
        String path = String.join("/", config.repositoryPath, datasetId, attempt, verbatim);
        int sparkParallelism = computeSparkParallelism(path, recordsNumber);

        LOG.info("Start the process. Message - {}", message);

        // Assembles a terminal java process and runs it
        int exitValue = ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .inputPath(path)
            .sparkParallelism(sparkParallelism)
            .build()
            .start()
            .waitFor();

        if (exitValue != 0) {
          LOG.error("Process has been finished with exit value - {}", exitValue);
        } else {
          LOG.info("Process has been finished with exit value - {}", exitValue);
        }

      } catch (InterruptedException | IOException ex) {
        LOG.error(ex.getMessage(), ex);
        throw new IllegalStateException("Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  /**
   * TODO:!
   */
  private int computeSparkParallelism(String verbatimPath, long recordsNumber) throws IOException {

    // Strategy 1: Chooses a runner type by number of records in a dataset
    if (recordsNumber > 0) {
      return (int) Math.ceil((double) recordsNumber / (double) config.sparkRecordsPerThread);
    }

    // Strategy 2: Chooses a runner type by calculating verbatim.avro file size
    long fileSizeByte = HdfsUtils.getfileSizeByte(verbatimPath, config.hdfsSiteConfig);
    int numberOfThreads = (int) Math.ceil(fileSizeByte / (config.threadPerMb * 1024d * 1024d));
    return numberOfThreads > 1 ? numberOfThreads : 1;
  }

  /**
   * Deletes directories if a dataset with the same attempt was interpreted before
   */
  private void deleteInterpretationsIfExist(PipelinesVerbatimMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    Set<String> steps = message.getInterpretTypes();

    if (steps != null && !steps.isEmpty()) {

      String path = String.join("/", config.repositoryPath, datasetId, attempt, Interpretation.DIRECTORY_NAME);

      if (steps.contains(ALL.name())) {
        HdfsUtils.deleteIfExist(config.hdfsSiteConfig, path);
      } else {
        for (String step : steps) {
          HdfsUtils.deleteIfExist(config.hdfsSiteConfig, String.join("/", path, step.toLowerCase()));
        }
      }
    }
  }

  private long getRecordNumber(PipelinesVerbatimMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);

    String recordsNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.DWCA_TO_AVRO_COUNT);
    return Long.parseLong(recordsNumber);
  }
}
