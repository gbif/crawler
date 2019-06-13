package org.gbif.crawler.pipelines.balancer.handler;

import java.io.IOException;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.crawler.pipelines.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback.Runner;
import org.gbif.crawler.pipelines.balancer.BalancerConfiguration;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Populates and sends the {@link PipelinesVerbatimMessage} message, the main method
 * is {@link VerbatimMessageHandler#handle}
 */
public class VerbatimMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(VerbatimMessageHandler.class);

  private VerbatimMessageHandler() {
    // NOP
  }

  /**
   * Main handler, basically computes the runner type and sends to the same consumer
   */
  public static void handle(BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    LOG.info("Process PipelinesVerbatimMessage - {}", message);

    // Populate message fields
    ObjectMapper mapper = new ObjectMapper();
    PipelinesVerbatimMessage m = mapper.readValue(message.getPayload(), PipelinesVerbatimMessage.class);
    long recordsNumber = getRecordNumber(config, m);
    String runner = computeRunner(config, m, recordsNumber).name();

    ValidationResult result = m.getValidationResult();
    if (result.getNumberOfRecords() == null) {
      result.setNumberOfRecords(recordsNumber);
    }

    PipelinesVerbatimMessage outputMessage =
        new PipelinesVerbatimMessage(m.getDatasetUuid(), m.getAttempt(), m.getInterpretTypes(), m.getPipelineSteps(),
            runner, m.getEndpointType(), m.getExtraPath(), result);

    publisher.send(outputMessage);

    LOG.info("The message has been sent - {}", outputMessage);
  }

  /**
   * Computes runner type:
   * Strategy 1 - Chooses a runner type by number of records in a dataset
   * Strategy 2 - Chooses a runner type by calculating verbatim.avro file size
   */
  private static Runner computeRunner(BalancerConfiguration config, PipelinesVerbatimMessage message,
      long recordsNumber)
      throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = String.valueOf(message.getAttempt());

    Runner runner;

    // Strategy 1: Chooses a runner type by number of records in a dataset
    if (recordsNumber > 0) {
      runner = recordsNumber >= config.switchRecordsNumber ? Runner.DISTRIBUTED : Runner.STANDALONE;
      LOG.info("Records number - {}, Spark Runner type - {}", recordsNumber, runner);
      return runner;
    }

    // Strategy 2: Chooses a runner type by calculating verbatim.avro file size
    String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
    String verbatimPath = String.join("/", config.repositoryPath, datasetId, attempt, verbatim);
    long fileSizeByte = HdfsUtils.getfileSizeByte(verbatimPath, config.hdfsSiteConfig);
    if (fileSizeByte > 0) {
      long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
      runner = fileSizeByte > switchFileSizeByte ? Runner.DISTRIBUTED : Runner.STANDALONE;
      LOG.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);
      return runner;
    }

    throw new IllegalStateException("Runner computation is failed " + datasetId);
  }

  /**
   * Reads number of records from a archive-to-avro metadata file
   */
  private static long getRecordNumber(BalancerConfiguration config, PipelinesVerbatimMessage message)
      throws IOException {

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
