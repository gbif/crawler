package org.gbif.crawler.pipelines.dwca;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.base.Preconditions;

import static org.gbif.api.vocabulary.DatasetType.OCCURRENCE;
import static org.gbif.crawler.constants.PipelinesNodePaths.DWCA_TO_VERBATIM;
import static org.gbif.crawler.pipelines.HdfsUtils.buildOutputPath;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesDwcaMessage} is received.
 */
public class DwcaToAvroCallback extends AbstractMessageCallback<PipelinesDwcaMessage> {

  private final DwcaToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  DwcaToAvroCallback(DwcaToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesDwcaMessage} message
   */
  @Override
  public void handleMessage(PipelinesDwcaMessage message) {

    if (OCCURRENCE != message.getDatasetType()) {
      return;
    }

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    int attempt = message.getAttempt();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps))
        .curator(curator)
        .zkRootElementPath(DWCA_TO_VERBATIM)
        .pipelinesStepName(Steps.DWCA_TO_VERBATIM.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();
  }

  /**
   * Main message processing logic, converts a DwCA archive to an avro file.
   */
  private Runnable createRunnable(PipelinesDwcaMessage message) {
    return () -> {

      UUID datasetId = message.getDatasetUuid();
      String attempt = String.valueOf(message.getAttempt());

      //calculates and checks existence of DwC Archive
      Path inputPath = buildInputPath(config.archiveRepository, datasetId);
      //calculates export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.fileName);

      org.apache.hadoop.fs.Path metaPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      DwcaToAvroConverter.create()
          .codecFactory(config.avroConfig.getCodec())
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsSiteConfig(config.hdfsSiteConfig)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .convert();
    };
  }

  /**
   * Input path example - /mnt/auto/crawler/dwca/9bed66b3-4caa-42bb-9c93-71d7ba109dad
   */
  private Path buildInputPath(String archiveRepository, UUID dataSetUuid) {
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString());
    Preconditions.checkState(directoryPath.toFile().exists(), "Directory - %s does not exist!", directoryPath);

    return directoryPath;
  }

}
