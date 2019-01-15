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
import org.gbif.crawler.pipelines.interpret.ProcessRunnerBuilder.RunnerEnum;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesInterpretedMessage(datasetId, message.getAttempt(), steps))
        .curator(curator)
        .zkRootElementPath(VERBATIM_TO_INTERPRETED)
        .pipelinesStepName(Steps.VERBATIM_TO_INTERPRETED.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();

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

        String path = String.join("/", config.repositoryPath, datasetId, attempt, "verbatim.avro");
        int sparkParallelism = computeSparkParallelism(path, recordsNumber);
        RunnerEnum runner = computeRunnerEnum(datasetId, attempt, recordsNumber);

        LOG.info("Start the process. DatasetId - {}, InterpretTypes - {}, Runner type - {}", datasetId,
            message.getInterpretTypes(), runner);

        // Assembles a terminal java process and runs it
        int exitValue = ProcessRunnerBuilder.create()
            .runner(runner)
            .config(config)
            .message(message)
            .inputPath(path)
            .sparkParallelism(sparkParallelism)
            .build()
            .start()
            .waitFor();

        LOG.info("Process has been finished with exit value - {}", exitValue);

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
   * TODO:!
   */
  private RunnerEnum computeRunnerEnum(String datasetId, String attempt, long recordsNumber) throws IOException {

    RunnerEnum runner;

    // Strategy 1: Chooses a runner type by number of records in a dataset
    if (recordsNumber > 0) {
      runner = recordsNumber >= config.switchRecordsNumber ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
      LOG.info("Records number - {}, Spark Runner type - {}", recordsNumber, runner);
      return runner;
    }

    // Strategy 2: Chooses a runner type by calculating verbatim.avro file size
    String verbatimPath = String.join("/", config.repositoryPath, datasetId, attempt, "verbatim.avro");
    long fileSizeByte = HdfsUtils.getfileSizeByte(verbatimPath, config.hdfsSiteConfig);
    if (fileSizeByte > 0) {
      long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
      runner = fileSizeByte > switchFileSizeByte ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
      LOG.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);
      return runner;
    }

    throw new IllegalStateException("Failed interpretation on " + datasetId);
  }

  /**
   * Deletes directories if a dataset with the same attempt was interpreted before
   */
  private void deleteInterpretationsIfExist(PipelinesVerbatimMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    Set<String> steps = message.getInterpretTypes();

    if (steps != null && !steps.isEmpty()) {

      String path = String.join("/", config.repositoryPath, datasetId, attempt, "interpreted");

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
