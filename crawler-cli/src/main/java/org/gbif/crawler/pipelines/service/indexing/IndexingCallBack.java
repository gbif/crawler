package org.gbif.crawler.pipelines.service.indexing;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.IndexDatasetMessage;
import org.gbif.crawler.pipelines.config.IndexingConfiguration;
import org.gbif.crawler.pipelines.service.indexing.ProcessRunnerBuilder.RunnerEnum;

import java.io.IOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link DwcaValidationFinishedMessage } is received.
 */
public class IndexingCallBack extends AbstractMessageCallback<IndexDatasetMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingCallBack.class);
  private final IndexingConfiguration config;

  IndexingCallBack(IndexingConfiguration config) {
    Objects.requireNonNull(config, "Configuration cannot be null");
    this.config = config;
  }

  @Override
  public void handleMessage(IndexDatasetMessage message) {
    LOG.info("Message received: {}", message);

    String datasetId = message.getDatasetUuid().toString();
    int attempt = message.getAttempt();

    try {

      // Chooses a runner type
      int filesCount = getfileCount(config);
      RunnerEnum runner = config.switchFilesNumber > filesCount ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
      LOG.info("Spark Runner type - {}", runner);

      // Number of Spark threads
      config.sparkParallelism = filesCount;

      String errorDirectory = config.processErrorDirectory;
      String error = errorDirectory != null ? errorDirectory + datasetId + "-idx-error.log" : null;
      LOG.info("Error file - {}", error);

      String outputDirectory = config.processOutputDirectory;
      String output = outputDirectory != null ? outputDirectory + datasetId + "-idx-output.log" : null;
      LOG.info("Output file - {}", output);

      // Assembles a process and runs it
      ProcessRunnerBuilder.create()
        .config(config)
        .message(message)
        .runner(runner)
        .esIndexName(datasetId + "_" + attempt)
        .esAlias(datasetId)
        .redirectOutputFile(output)
        .redirectErrorFile(error)
        .build()
        .start()
        .waitFor();

      LOG.info("Finish the process. DatasetId - {}, Attempt - {}, Runner type - {}", datasetId, attempt, runner);

    } catch (InterruptedException | IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new IllegalStateException("Failed performing interpretation on " + datasetId, ex);
    }
  }

  private static int getfileCount(IndexingConfiguration config) throws IOException {
    return 1;
  }

}
