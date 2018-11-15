package org.gbif.crawler.pipelines.service.indexing;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.IndexComplitedMessage;
import org.gbif.common.messaging.api.messages.IndexDatasetMessage;
import org.gbif.crawler.pipelines.config.IndexingConfiguration;
import org.gbif.crawler.pipelines.service.indexing.ProcessRunnerBuilder.RunnerEnum;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link DwcaValidationFinishedMessage } is received.
 */
public class IndexingCallback extends AbstractMessageCallback<IndexDatasetMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingCallback.class);
  private final IndexingConfiguration config;
  private final MessagePublisher publisher;

  IndexingCallback(IndexingConfiguration config, MessagePublisher publisher) {
    Objects.requireNonNull(config, "Configuration cannot be null");
    this.config = config;
    this.publisher = publisher;
  }

  @Override
  public void handleMessage(IndexDatasetMessage message) {
    LOG.info("Message received: {}", message);

    String datasetId = message.getDatasetUuid().toString();
    int attempt = message.getAttempt();

    try {
      // Chooses a runner type
      int filesCount = getfileCount(config, message);
      RunnerEnum runner = filesCount > config.switchFilesNumber ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
      LOG.info("Number of files - {}, Spark Runner type - {}", filesCount, runner);

      // Number of Spark threads
      config.sparkParallelism = filesCount;

      // Assembles a process and runs it
      ProcessRunnerBuilder.create()
        .config(config)
        .message(message)
        .runner(runner)
        .esIndexName(datasetId + "_" + attempt)
        .esAlias(datasetId + "," + config.idxAlias)
        .build()
        .start()
        .waitFor();

      LOG.info("Finish the process. DatasetId - {}, Attempt - {}, Runner type - {}", datasetId, attempt, runner);

      // Send message to MQ
      if (Objects.nonNull(publisher)) {
        try {
          publisher.send(new IndexComplitedMessage(message.getDatasetUuid(), attempt));
          LOG.info("Message has been sent DatasetId - {}, Attempt - {}", datasetId, attempt);
        } catch (IOException e) {
          LOG.error("Could not send message for DatasetId [{}] : {}", datasetId, e.getMessage());
        }
      }

    } catch (InterruptedException | IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new IllegalStateException("Failed performing interpretation on " + datasetId, ex);
    }
  }

  private static int getfileCount(IndexingConfiguration config, IndexDatasetMessage message) throws IOException {

    String path = String.join("/", config.targetDirectory, message.getDatasetUuid().toString(),
      Integer.toString(message.getAttempt()), "basic");
    String hdfsSiteConfig = config.hdfsSiteConfig;

    FileSystem fs;
    try {
      Configuration configuration = new Configuration();

      // check if the hdfs-site.xml is provided
      if (!Strings.isNullOrEmpty(hdfsSiteConfig)) {
        File hdfsSite = new File(hdfsSiteConfig);
        if (hdfsSite.exists() && hdfsSite.isFile()) {
          LOG.info("using hdfs-site.xml");
          configuration.addResource(hdfsSite.toURI().toURL());
        } else {
          LOG.warn("hdfs-site.xml does not exist");
        }
      }

      URI basicRepository = URI.create(path);
      fs = FileSystem.get(basicRepository, configuration);
    } catch (IOException ex) {
      throw new IllegalStateException("Can't get a valid filesystem from provided uri " + path, ex);
    }

    int count = 0;
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(path), false);
    while (iterator.hasNext()) {
      LocatedFileStatus fileStatus = iterator.next();
      if (fileStatus.isFile()) {
        count++;
      }
    }
    return count;
  }

}
