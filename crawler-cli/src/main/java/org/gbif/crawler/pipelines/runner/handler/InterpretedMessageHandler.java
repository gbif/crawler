package org.gbif.crawler.pipelines.runner.handler;

import java.io.IOException;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.pipelines.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback.Runner;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.crawler.pipelines.runner.BalancerConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class InterpretedMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretedMessageHandler.class);

  private InterpretedMessageHandler() {
  }

  /**
   * TODO: DOC
   */
  public static void handle(BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    LOG.info("Process PipelinesInterpretedMessage - {}", message);

    ObjectMapper mapper = new ObjectMapper();
    PipelinesInterpretedMessage m = mapper.readValue(message.getPayload(), PipelinesInterpretedMessage.class);
    String runner = computeRunner(config, m).name();
    publisher.send(new PipelinesInterpretedMessage(m.getDatasetUuid(), m.getAttempt(), m.getPipelineSteps(), runner));
  }

  /**
   * TODO: DOC
   */
  private static Runner computeRunner(BalancerConfiguration config, PipelinesInterpretedMessage message)
      throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = String.valueOf(message.getAttempt());
    long recordsNumber = getRecordNumber(config, message);

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
   * TODO: DOC
   */
  private static long getRecordNumber(BalancerConfiguration config, PipelinesInterpretedMessage message)
      throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);

    String recordsNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.DWCA_TO_AVRO_COUNT);
    return Long.parseLong(recordsNumber);
  }

}
