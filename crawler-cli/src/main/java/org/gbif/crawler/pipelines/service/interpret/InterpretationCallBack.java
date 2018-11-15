package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.common.messaging.api.messages.IndexDatasetMessage;
import org.gbif.crawler.pipelines.config.InterpreterConfiguration;
import org.gbif.crawler.pipelines.service.interpret.ProcessRunnerBuilder.RunnerEnum;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage } is received.
 */
public class InterpretationCallback extends AbstractMessageCallback<ExtendedRecordAvailableMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationCallback.class);
  private final InterpreterConfiguration config;
  private final MessagePublisher publisher;

  InterpretationCallback(InterpreterConfiguration config, MessagePublisher publisher) {
    Objects.requireNonNull(config, "Configuration cannot be null");
    this.config = config;
    this.publisher = publisher;
  }

  @Override
  public void handleMessage(ExtendedRecordAvailableMessage message) {
    LOG.info("Message received: {}", message);

    UUID datasetId = message.getDatasetUuid();
    int attempt = message.getAttempt();

    try {

      // Chooses a runner type
      long fileSizeByte = getfileSizeByte(message.getInputFile(), config.hdfsSiteConfig);
      long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
      RunnerEnum runner = fileSizeByte > switchFileSizeByte ? RunnerEnum.DISTRIBUTED : RunnerEnum.STANDALONE;
      LOG.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);

      // Number of Spark threads
      int numberOfTreads = (int) Math.ceil(fileSizeByte / (20d * 1024d * 1024d)); // 1 thread per 20MB
      config.sparkParallelism = numberOfTreads > 1 ? numberOfTreads : 1;

      LOG.info("Start the process. DatasetId - {}, InterpretTypes - {}, Runner type - {}", datasetId,
        message.getInterpretTypes(), runner);

      // Assembles a process and runs it
      ProcessRunnerBuilder.create()
        .runner(runner)
        .config(config)
        .message(message)
        .build()
        .start()
        .waitFor();

      LOG.info("Finish the process. DatasetId - {}, InterpretTypes - {}, Runner type - {}", datasetId,
        message.getInterpretTypes(), runner);

      // Send message to MQ
      if (Objects.nonNull(publisher)) {
        try {
          publisher.send(new IndexDatasetMessage(datasetId, attempt));
          LOG.info("Message has been sent DatasetId - {}, Attempt - {}", datasetId, attempt);
        } catch (IOException e) {
          LOG.error("Could not send message for DatasetId [{}] : {}", datasetId, e.getMessage());
        }
      }

    } catch (InterruptedException | IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new IllegalStateException("Failed performing interpretation on " + datasetId.toString(), ex);
    }
  }

  private static long getfileSizeByte(URI file, String hdfsSiteConfig) throws IOException {
    FileSystem fs;
    try {
      Configuration config = new Configuration();

      // check if the hdfs-site.xml is provided
      if (!Strings.isNullOrEmpty(hdfsSiteConfig)) {
        File hdfsSite = new File(hdfsSiteConfig);
        if (hdfsSite.exists() && hdfsSite.isFile()) {
          LOG.info("using hdfs-site.xml");
          config.addResource(hdfsSite.toURI().toURL());
        } else {
          LOG.warn("hdfs-site.xml does not exist");
        }
      }

      URI extendedRecordRepository = URI.create(file.toString());
      fs = FileSystem.get(extendedRecordRepository, config);
    } catch (IOException ex) {
      throw new IllegalStateException("Can't get a valid filesystem from provided uri " + file.toString(), ex);
    }

    return fs.getFileStatus(new Path(file)).getLen();
  }
}
