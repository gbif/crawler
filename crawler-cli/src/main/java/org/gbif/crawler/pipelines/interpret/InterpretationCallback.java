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

    MDC.put("datasetId", message.getDatasetUuid().toString());
    MDC.put("attempt", String.valueOf(message.getAttempt()));
    LOG.info("Message handler began - {}", message);

    if (!isMessageCorrect(message)) {
      return;
    }

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    Long recordsNumber = null;
    if (message.getValidationResult() != null && message.getValidationResult().getNumberOfRecords() != null) {
      recordsNumber = message.getValidationResult().getNumberOfRecords();
    }

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesInterpretedMessage(datasetId, message.getAttempt(), steps, recordsNumber, null))
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
        int sparkParallelism = computeSparkParallelism(path, recordsNumber);
        int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);
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
   * Compute the number of thread for spark.default.parallelism, top limit is config.sparkParallelismMax
   * Remember YARN will create the same number of files
   */
  private int computeSparkParallelism(String verbatimPath, long recordsNumber) throws IOException {

    // Strategy 1: Chooses a runner type by number of records in a dataset
    if (recordsNumber > 0) {
      int count = (int) Math.ceil((double) recordsNumber / (double) config.sparkRecordsPerThread);
      return count > config.sparkParallelismMax ? config.sparkParallelismMax : count;
    }

    // Strategy 2: Chooses a runner type by calculating verbatim.avro file size
    long fileSizeByte = HdfsUtils.getfileSizeByte(verbatimPath, config.hdfsSiteConfig);
    int numberOfThreads = (int) Math.ceil(fileSizeByte / (config.threadPerMb * 1024d * 1024d));
    return (numberOfThreads > 1 && numberOfThreads > config.sparkParallelismMin) ? numberOfThreads :
        config.sparkParallelismMin;
  }


  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and
   * max is config.sparkExecutorMemoryGbMax
   * <p>
   * 231_168d is found empirically salt 192d
   */
  private String computeSparkExecutorMemory(long recordsNumber, int sparkExecutorNumbers) {
    int memoryGb = (int) Math.ceil(recordsNumber / (double) sparkExecutorNumbers / 231_168d);
    memoryGb = memoryGb < config.sparkExecutorMemoryGbMin ? config.sparkExecutorMemoryGbMin :
        memoryGb > config.sparkExecutorMemoryGbMax ? config.sparkExecutorMemoryGbMax : memoryGb;
    return memoryGb + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkExecutorNumbersMin and
   * max is config.sparkExecutorNumbersMax
   * <p>
   * 500_000d is records per executor
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers = (int) Math.ceil(recordsNumber / 500_000d);
    return sparkExecutorNumbers < config.sparkExecutorNumbersMin ? config.sparkExecutorNumbersMin :
        sparkExecutorNumbers > config.sparkExecutorNumbersMax ? config.sparkExecutorNumbersMax : sparkExecutorNumbers;
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
}
