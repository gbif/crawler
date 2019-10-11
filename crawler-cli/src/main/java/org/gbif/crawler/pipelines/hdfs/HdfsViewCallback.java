package org.gbif.crawler.pipelines.hdfs;

import java.io.IOException;

import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.pipelines.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.crawler.pipelines.indexing.IndexingConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.google.common.base.Strings;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 * <p>
 * The main method is {@link HdfsViewCallback#handleMessage}
 */
public class HdfsViewCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsViewCallback.class);
  private static final StepType STEP = StepType.HDFS_VIEW;

  private final HdfsViewConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient historyWsClient;

  HdfsViewCallback(HdfsViewConfiguration config, MessagePublisher publisher, CuratorFramework curator,
      PipelinesHistoryWsClient historyWsClient) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
    this.historyWsClient = historyWsClient;
  }

  /** Handles a MQ {@link PipelinesInterpretedMessage} message */
  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", message.getDatasetUuid().toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDCCloseable mdc3 = MDC.putCloseable("step", STEP.name())) {

      if (!isMessageCorrect(message)) {
        LOG.info("Skip the message, cause the runner is different or it wasn't modified, exit from handler");
        return;
      }

      LOG.info("Message handler began - {}", message);

      Runnable runnable = createRunnable(message);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .curator(curator)
          .zkRootElementPath(STEP.getLabel())
          .pipelinesStepName(STEP)
          .publisher(publisher)
          .runnable(runnable)
          .historyWsClient(historyWsClient)
          .build()
          .handleMessage();

      LOG.info("Message handler ended - {}", message);

    }
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs
   */
  private Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      try {

        ProcessRunnerBuilder builder = ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .numberOfShards(computeNumberOfShards(message));

        if (config.processRunner.equalsIgnoreCase(StepRunner.DISTRIBUTED.name())) {

          long recordNumber = getRecordNumber(message);
          int sparkExecutorNumbers = computeSparkExecutorNumbers(recordNumber);

          builder.sparkParallelism(computeSparkParallelism(sparkExecutorNumbers))
              .sparkExecutorMemory(computeSparkExecutorMemory(sparkExecutorNumbers))
              .sparkExecutorNumbers(sparkExecutorNumbers);
        }

        // Assembles a terminal java process and runs it
        int exitValue = builder.build().start().waitFor();

        if (exitValue != 0) {
          throw new RuntimeException("Process has been finished with exit value - " + exitValue);
        } else {
          LOG.info("Process has been finished with exit value - {}", exitValue);
        }

      } catch (InterruptedException | IOException ex) {
        LOG.error(ex.getMessage(), ex);
        throw new IllegalStateException("Failed indexing on " + message.getDatasetUuid(), ex);
      }
    };
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in service config
   * {@link IndexingConfiguration#processRunner}
   */
  private boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message.toString());
    }
    if (message.getOnlyForStep() != null && !message.getOnlyForStep().equalsIgnoreCase(STEP.name())) {
      return false;
    }
    return config.processRunner.equals(message.getRunner());
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is config.sparkParallelismMax
   * Remember YARN will create the same number of files
   */
  private int computeSparkParallelism(int executorNumbers) {
    int count = executorNumbers * config.sparkExecutorCores * 2;

    if (count < config.sparkParallelismMin) {
      return config.sparkParallelismMin;
    }
    if (count > config.sparkParallelismMax) {
      return config.sparkParallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and
   * max is config.sparkExecutorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers) {

    if (sparkExecutorNumbers < config.sparkExecutorMemoryGbMin) {
      return config.sparkExecutorMemoryGbMin + "G";
    }
    if (sparkExecutorNumbers > config.sparkExecutorMemoryGbMax) {
      return config.sparkExecutorMemoryGbMax + "G";
    }
    return sparkExecutorNumbers + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkExecutorNumbersMin and
   * max is config.sparkExecutorNumbersMax
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers =
        (int) Math.ceil(recordsNumber / (config.sparkExecutorCores * config.sparkRecordsPerThread * 1f));
    if (sparkExecutorNumbers < config.sparkExecutorNumbersMin) {
      return config.sparkExecutorNumbersMin;
    }
    if (sparkExecutorNumbers > config.sparkExecutorNumbersMax) {
      return config.sparkExecutorNumbersMax;
    }
    return sparkExecutorNumbers;
  }

  /**
   * Reads number of records from a archive-to-avro metadata file, verbatim-to-interpreted contains attempted records
   * count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);

    String recordsNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.ARCHIVE_TO_ER_COUNT);
    if (recordsNumber == null || recordsNumber.isEmpty()) {
      if (message.getNumberOfRecords() != null) {
        return message.getNumberOfRecords();
      } else {
        throw new IllegalArgumentException(
            "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
      }
    }
    return Long.parseLong(recordsNumber);
  }

  private int computeNumberOfShards(PipelinesInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String dirPath = String.join("/", config.repositoryPath, datasetId, attempt, DIRECTORY_NAME);
    long sizeByte = HdfsUtils.getFileSizeByte(dirPath, config.hdfsSiteConfig);
    if (sizeByte == -1d) {
      throw new IllegalArgumentException("Please check interpretation source directory! - " + dirPath);
    }
    long sizeExpected = config.hdfsAvroExpectedFileSizeInMb * 1048576L; // 1024 * 1024
    double numberOfShards = (sizeByte * config.hdfsAvroCoefficientRatio / 100f) / sizeExpected;
    double numberOfShardsFloor = Math.floor(numberOfShards);
    numberOfShards = numberOfShards - numberOfShardsFloor > 0.5d ? numberOfShardsFloor + 1 : numberOfShardsFloor;
    return numberOfShards <= 0 ? 1 : (int) numberOfShards;
  }


}
