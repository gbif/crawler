package org.gbif.crawler.pipelines.service.dwca;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;
import org.gbif.crawler.pipelines.service.PipelineCallback;

import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;

import static org.gbif.crawler.constants.PipelinesNodePaths.DWCA_TO_VERBATIM;
import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.DWCA;
import static org.gbif.crawler.pipelines.service.PipelineCallback.Steps.VERBATIM_TO_INTERPRETED;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesDwcaMessage} is received.
 */
public class DwcaToAvroCallback extends AbstractMessageCallback<PipelinesDwcaMessage> {

  private final ConverterConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  DwcaToAvroCallback(ConverterConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesDwcaMessage} message
   */
  @Override
  public void handleMessage(PipelinesDwcaMessage message) {

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    int attempt = message.getAttempt();
    Set<String> steps = message.getPipelineSteps();

    // Main message processing logic, converts a DwCA archive to an avro file.
    Runnable runnable = () -> {
      ArchiveToAvroPath paths = PathFactory.create(DWCA).from(config, datasetId, attempt);
      DwcaToAvroConverter.create()
        .codecFactory(config.avroConfig.getCodec())
        .syncInterval(config.avroConfig.syncInterval)
        .hdfsSiteConfig(config.hdfsSiteConfig)
        .convert(paths.getInputPath(), paths.getOutputPath());
    };

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
      .incomingMessage(message)
      .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps))
      .curator(curator)
      .zkRootElementPath(DWCA_TO_VERBATIM)
      .nextPipelinesStep(VERBATIM_TO_INTERPRETED.name())
      .publisher(publisher)
      .runnable(runnable)
      .build()
      .handleMessage();
  }

}
