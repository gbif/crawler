package org.gbif.crawler.pipelines.interpret;

import java.io.IOException;
import java.util.Optional;
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
import org.slf4j.MDC;

import com.google.common.base.Strings;

import static org.gbif.crawler.constants.PipelinesNodePaths.VERBATIM_TO_INTERPRETED;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesVerbatimMessage} is received.
 * <p>
 * The main method is {@link InterpretationCallback#handleMessage}
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

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = Optional.ofNullable(message.getAttempt()).orElseGet(() -> getLatestAttempt(message));

    MDC.put("datasetId", datasetId.toString());
    MDC.put("attempt", attempt.toString());
    LOG.info("Message handler began - {}", message);

    if (!isMessageCorrect(message)) {
      return;
    }

    // Common variables
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    Long recordsNumber = null;
    if (message.getValidationResult() != null && message.getValidationResult().getNumberOfRecords() != null) {
      recordsNumber = message.getValidationResult().getNumberOfRecords();
    }

    boolean repeatAttempt = pathExists(message);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesInterpretedMessage(datasetId, attempt, steps, recordsNumber, repeatAttempt))
        .curator(curator)
        .zkRootElementPath(VERBATIM_TO_INTERPRETED)
        .pipelinesStepName(Steps.VERBATIM_TO_INTERPRETED.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();

    LOG.info("Message handler ended - {}", message);

  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in service config
   * {@link InterpreterConfiguration#processRunner}
   */
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

        long recordsNumber = getRecordNumber(message);

        String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
        String path = message.getExtraPath() != null ? message.getExtraPath() :
            String.join("/", config.repositoryPath, datasetId, attempt, verbatim);
        int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);
        int sparkParallelism = computeSparkParallelism(sparkExecutorNumbers);
        String sparkExecutorMemory = computeSparkExecutorMemory(recordsNumber, sparkExecutorNumbers);

        LOG.info("Start the process. Message - {}", message);

        // Assembles a terminal java process and runs it
        int exitValue = ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .inputPath(path)
            .sparkParallelism(sparkParallelism)
            .sparkExecutorMemory(sparkExecutorMemory)
            .sparkExecutorNumbers(sparkExecutorNumbers)
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
        throw new IllegalStateException("Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is config.sparkParallelismMax
   * Remember YARN will create the same number of files
   */
  private int computeSparkParallelism(int executorNumbers) {
    int count = executorNumbers * config.sparkExecutorCores * 2;

    if(count < config.sparkParallelismMin) {
      return config.sparkParallelismMin;
    }
    if(count > config.sparkParallelismMax){
      return config.sparkParallelismMax;
    }
    return count;
  }


  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and
   * max is config.sparkExecutorMemoryGbMax
   * <p>
   * 231_168d is found empirically salt 192d
   */
  private String computeSparkExecutorMemory(long recordsNumber, int sparkExecutorNumbers) {
    int memoryGb = (int) Math.ceil(recordsNumber / (double) sparkExecutorNumbers / 231_168d);

    if(memoryGb < config.sparkExecutorMemoryGbMin) {
      return config.sparkExecutorMemoryGbMin + "G";
    }
    if(memoryGb > config.sparkExecutorMemoryGbMax){
      return config.sparkExecutorMemoryGbMax + "G";
    }
    return memoryGb + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkExecutorNumbersMin and
   * max is config.sparkExecutorNumbersMax
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers = (int) Math.ceil(recordsNumber / (config.sparkExecutorCores * config.sparkRecordsPerThread));
    if(sparkExecutorNumbers < config.sparkExecutorNumbersMin) {
      return config.sparkExecutorNumbersMin;
    }
    if(sparkExecutorNumbers > config.sparkExecutorNumbersMax){
      return config.sparkExecutorNumbersMax;
    }
    return sparkExecutorNumbers;
  }

  /**
   * Reads number of records from the message or archive-to-avro metadata file
   */
  private long getRecordNumber(PipelinesVerbatimMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);

    String recordsNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.ARCHIVE_TO_ER_COUNT);
    if (recordsNumber == null || recordsNumber.isEmpty()) {
      if (message.getValidationResult() != null && message.getValidationResult().getNumberOfRecords() != null) {
        return message.getValidationResult().getNumberOfRecords();
      } else {
        throw new IllegalArgumentException(
            "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
      }
    }
    return Long.parseLong(recordsNumber);
  }

  /**
   * Checks if the directory exists
   */
  private boolean pathExists(PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String path = String.join("/", config.repositoryPath, datasetId, attempt, Interpretation.DIRECTORY_NAME);
    try {
      return HdfsUtils.exists(config.hdfsSiteConfig, path);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Finds the latest attempt number in HDFS
   */
  private Integer getLatestAttempt(PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String path = String.join("/", config.repositoryPath, datasetId);
    try {
      return HdfsUtils.getSubDirList(config.hdfsSiteConfig, path)
          .stream()
          .filter(x -> x.chars().allMatch(Character::isDigit))
          .mapToInt(Integer::valueOf)
          .max()
          .orElseThrow(() -> new RuntimeException("Can't find the maximum attempt"));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
