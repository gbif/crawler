package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.crawler.pipelines.FileSystemUtils;
import org.gbif.crawler.pipelines.config.InterpreterConfiguration;
import org.gbif.crawler.pipelines.service.interpret.ProcessRunnerBuilder.RunnerEnum;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage } is received.
 */
public class InterpretationCallBack extends AbstractMessageCallback<ExtendedRecordAvailableMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationCallBack.class);
  private final InterpreterConfiguration config;

  InterpretationCallBack(InterpreterConfiguration config) {
    this.config = config;
  }

  @Override
  public void handleMessage(ExtendedRecordAvailableMessage message) {
    LOG.info("Message received: {}", message);

    UUID datasetId = message.getDatasetUuid();

    try {

      // Chooses a runner type
      long fileSize = FileSystemUtils.fileSize(message.getInputFile(), config.hdfsSiteConfig);
      RunnerEnum runner = fileSize > config.switchFileSize ? RunnerEnum.SPARK : RunnerEnum.DIRECT;
      LOG.info("Runner type - {}", runner);

      // Assembles a process and runs it
      LOG.info("Start the process. DatasetId - {}, InterpretTypes - {}, Runner type - {}",
               datasetId, message.getInterpretTypes(), runner);

      String error = Objects.nonNull(config.processErrorDirectory) ? config.processErrorDirectory + datasetId + "-err.log" : null;
      LOG.info("Error file - {}", error);
      String output = Objects.nonNull(config.processOutputDirectory) ? config.processOutputDirectory + datasetId + "-out.log" : null;
      LOG.info("Output file - {}", output);

      ProcessRunnerBuilder.create(config)
        .runner(runner)
        .datasetId(datasetId.toString())
        .attempt(message.getAttempt())
        .inputFile(message.getInputFile().toString())
        .interpretationTypes(message.getInterpretTypes())
        .redirectOutputFile(output)
        .redirectErrorFile(error)
        .build()
        .start()
        .waitFor();

      LOG.info("Finish the process. DatasetId - {}, InterpretTypes - {}, Runner type - {}",
               datasetId, message.getInterpretTypes(), runner);

    } catch (InterruptedException | IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new IllegalStateException("Failed performing interpretation on " + datasetId.toString(), ex);
    }
  }
}
