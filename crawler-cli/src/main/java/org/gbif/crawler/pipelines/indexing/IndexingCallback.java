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
import org.gbif.crawler.pipelines.indexing.ProcessRunnerBuilder.RunnerEnum;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.PipelinesNodePaths.INTERPRETED_TO_INDEX;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 */
public class IndexingCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingCallback.class);
  private final IndexingConfiguration config;
  private final MessagePublisher publisher;
  private final DatasetService datasetService;
  private final CuratorFramework curator;

  IndexingCallback(IndexingConfiguration config, MessagePublisher publisher, DatasetService datasetService, CuratorFramework curator) {
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

    UUID datasetId = message.getDatasetUuid();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesIndexedMessage(datasetId, message.getAttempt(), steps))
        .curator(curator)
        .zkRootElementPath(INTERPRETED_TO_INDEX)
        .pipelinesStepName(Steps.INTERPRETED_TO_INDEX.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();
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
        RunnerEnum runner = computeRunnerEnum(datasetId, attempt, recordsNumber, sparkParallelism);

        // Assembles a terminal java process and runs it
        int exitValue = ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .runner(runner)
            .sparkParallelism(sparkParallelism)
            .esIndexName(indexName)
            .esAlias(indexAlias)
            .esShardsNumber(numberOfShards)
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
        throw new IllegalStateException("Failed indexing on " + message.getDatasetUuid(), ex);
      }
    };
  }

  private int computeSparkParallelism(String datasetId, String attempt) throws IOException {
    // Chooses a runner type by calculating number of files
    String basicPath = String.join("/", config.repositoryPath, datasetId, attempt, "interpreted", "basic");
    return HdfsUtils.getfileCount(basicPath, config.hdfsSiteConfig);
  }

  /**
   * TODO:!
   */
  private RunnerEnum computeRunnerEnum(String datasetId, String attempt, long recordsNumber, int sparkParallelism)
      throws IOException {

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

    // Strategy 3: Chooses a runner type by number of files
    runner = sparkParallelism > config.switchFilesNumber ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
    LOG.info("Number of files - {}, Spark Runner type - {}", sparkParallelism, runner);
    return runner;
  }

  /**
   * TODO:!
   */
  private String computeIndexName(String datasetId, String attempt, long recordsNumber) {

    // Independent index for datasets where number of records more than config.indexIndepRecord
    String idxName;

    if (recordsNumber >= config.indexIndepRecord) {
      idxName = datasetId + "_" + attempt;
      LOG.info("ES Index name - {}, recordsNumber - {}, config.indexIndepRecord - {}", idxName, recordsNumber, config.indexIndepRecord);
      return idxName;
    }

    // Default static index name for datasets where last changed date more than config.indexDefStaticDateDurationDd
    Date lastChangedDate = getLastChangetDate(datasetId);

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
   * TODO:!
   */
  private Date getLastChangetDate(String datasetId) {
    Dataset dataset = datasetService.get(UUID.fromString(datasetId));
    return dataset.getModified();
  }

  /**
   * TODO:!
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
