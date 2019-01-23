package org.gbif.crawler.pipelines.indexing;

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.pipelines.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import static org.gbif.crawler.constants.PipelinesNodePaths.INTERPRETED_TO_INDEX;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 * <p>
 * The main method is {@link IndexingCallback#handleMessage}
 */
public class IndexingCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingCallback.class);
  private final IndexingConfiguration config;
  private final MessagePublisher publisher;
  private final DatasetService datasetService;
  private final CuratorFramework curator;

  IndexingCallback(IndexingConfiguration config, MessagePublisher publisher, DatasetService datasetService,
      CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.datasetService = checkNotNull(datasetService, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesInterpretedMessage} message
   */
  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {

    if (!isMessageCorrect(message)) {
      return;
    }

    UUID datasetId = message.getDatasetUuid();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesIndexedMessage(datasetId, message.getAttempt(), steps, null))
        .curator(curator)
        .zkRootElementPath(INTERPRETED_TO_INDEX)
        .pipelinesStepName(Steps.INTERPRETED_TO_INDEX.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in service config
   * {@link IndexingConfiguration#processRunner}
   */
  private boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message.toString());
    }
    return config.processRunner.equals(message.getRunner());
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index pipeline
   */
  private Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      try {
        String datasetId = message.getDatasetUuid().toString();
        String attempt = Integer.toString(message.getAttempt());

        long recordsNumber = getRecordNumber(message);

        String indexName = computeIndexName(datasetId, attempt, recordsNumber);
        String indexAlias = indexName.startsWith(datasetId) ? datasetId + "," + config.indexAlias : "";
        int numberOfShards = (int) Math.ceil((double) recordsNumber / (double) config.indexRecordsPerShard);
        int sparkParallelism = computeSparkParallelism(datasetId, attempt);

        // Assembles a terminal java process and runs it
        int exitValue = ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .sparkParallelism(sparkParallelism)
            .esIndexName(indexName)
            .esAlias(indexAlias)
            .esShardsNumber(numberOfShards)
            .build()
            .start()
            .waitFor();

        if (exitValue != 0) {
          LOG.error("Process has been finished with exit value - {}, dataset - {}_{}", exitValue, datasetId, attempt);
        } else {
          LOG.info("Process has been finished with exit value - {}, dataset - {}_{}", exitValue, datasetId, attempt);
        }

      } catch (InterruptedException | IOException ex) {
        LOG.error(ex.getMessage(), ex);
        throw new IllegalStateException("Failed indexing on " + message.getDatasetUuid(), ex);
      }
    };
  }

  /**
   * Computes the number of thread for spark.default.parallelism
   */
  private int computeSparkParallelism(String datasetId, String attempt) throws IOException {
    // Chooses a runner type by calculating number of files
    String basic = RecordType.BASIC.name().toLowerCase();
    String directoryName = Interpretation.DIRECTORY_NAME;
    String basicPath = String.join("/", config.repositoryPath, datasetId, attempt, directoryName, basic);
    return HdfsUtils.getfileCount(basicPath, config.hdfsSiteConfig);
  }

  /**
   * Computes the name for ES index:
   * Case 1 - Independent index for datasets where number of records more than config.indexIndepRecord
   * Case 2 - Default static index name for datasets where last changed date more than config.indexDefStaticDateDurationDd
   * Case 3 - Default dynamic index name for all other datasets
   */
  private String computeIndexName(String datasetId, String attempt, long recordsNumber) {

    // Independent index for datasets where number of records more than config.indexIndepRecord
    String idxName;

    if (recordsNumber >= config.indexIndepRecord) {
      idxName = datasetId + "_" + attempt;
      LOG.info("ES Index name - {}, recordsNumber - {}, config.indexIndepRecord - {}", idxName, recordsNumber,
          config.indexIndepRecord);
      return idxName;
    }

    // Default static index name for datasets where last changed date more than config.indexDefStaticDateDurationDd
    Date lastChangedDate = getLastChangedDate(datasetId);

    long diffInMillies = Math.abs(new Date().getTime() - lastChangedDate.getTime());
    long diff = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
    if (diff >= config.indexDefStaticDateDurationDd) {
      idxName = config.indexDefStaticName;
      LOG.info("ES Index name - {}, lastChangedDate - {}, diff days - {}", idxName, lastChangedDate, diff);
      return idxName;
    }

    // Default dynamic index name for all other datasets
    idxName = config.indexDefDynamicName;
    LOG.info("ES Index name - {}, lastChangedDate - {}, diff days - {}", idxName, lastChangedDate, diff);
    return idxName;
  }

  /**
   * Uses Registry to ask the last changed date for a dataset
   */
  private Date getLastChangedDate(String datasetId) {
    Dataset dataset = datasetService.get(UUID.fromString(datasetId));
    return dataset.getModified();
  }

  /**
   * Reads number of records from a dwca-to-avro metadata file, verbatim-to-interpreted contains attempted records
   * count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);

    String recordsNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.DWCA_TO_AVRO_COUNT);
    return Long.parseLong(recordsNumber);
  }
}
