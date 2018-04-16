package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.crawler.pipelines.FileSystemUtils;
import org.gbif.crawler.pipelines.config.InterpreterConfiguration;
import org.gbif.crawler.pipelines.service.interpret.ProcessRunnerBuilder.RunnerEnum;

import java.io.IOException;

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

    String uuid = message.getDatasetUuid().toString();

    try {

      long fileSize = FileSystemUtils.fileSize(message.getInputFile(), config.hdfsSiteConfig);
      RunnerEnum runner = fileSize > config.switchFileSize ? RunnerEnum.SPARK : RunnerEnum.DIRECT;

      ProcessRunnerBuilder.create(config)
        .runner(runner)
        .datasetId(uuid)
        .inputFile(message.getInputFile().toString())
        .interpretationTypes(message.getInterpretTypes())
        .build()
        .start()
        .waitFor();
    } catch (InterruptedException | IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new IllegalStateException("Failed performing interpretation on " + uuid, ex);
    } finally {
      // TODO: DELETE WRONG FILE
    }
  }
}
