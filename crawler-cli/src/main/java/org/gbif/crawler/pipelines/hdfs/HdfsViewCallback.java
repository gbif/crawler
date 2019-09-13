package org.gbif.crawler.pipelines.hdfs;

import java.io.IOException;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.pipelines.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import static org.gbif.api.model.pipelines.StepType.HDFS_VIEW;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 * <p>
 * The main method is {@link HdfsViewCallback#handleMessage}
 */
public class HdfsViewCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsViewCallback.class);
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
        MDCCloseable mdc3 = MDC.putCloseable("step", HDFS_VIEW.name())) {

      LOG.info("Message handler began - {}", message);

      Runnable runnable = createRunnable(message);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .curator(curator)
          .zkRootElementPath(HDFS_VIEW.getLabel())
          .pipelinesStepName(StepType.HDFS_VIEW)
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

        long recordsNumber = getRecordNumber(message);

        int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);
        int sparkParallelism = computeSparkParallelism(sparkExecutorNumbers);
        String sparkExecutorMemory = computeSparkExecutorMemory(sparkExecutorNumbers);
        int numberOfShards = computeNumberOfShards(message);

        // Assembles a terminal java process and runs it
        int exitValue = ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .sparkParallelism(sparkParallelism)
            .sparkExecutorMemory(sparkExecutorMemory)
            .sparkExecutorNumbers(sparkExecutorNumbers)
            .numberOfShards(numberOfShards)
            .build()
            .start()
            .waitFor();

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
