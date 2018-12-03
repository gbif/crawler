package org.gbif.crawler.pipelines.service.xml;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.converters.XmlToAvroConverter;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;
import org.gbif.crawler.pipelines.service.PipelineCallback;
import org.gbif.crawler.pipelines.service.PipelineCallback.Steps;

import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;

import static org.gbif.crawler.constants.PipelinesNodePaths.XML_TO_VERBATIM;
import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.XML;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 */
public class XmlToAvroCallback extends AbstractMessageCallback<PipelinesXmlMessage> {

  private final ConverterConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  public XmlToAvroCallback(ConverterConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesXmlMessage} message
   */
  @Override
  public void handleMessage(PipelinesXmlMessage message) {

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    int attempt = message.getAttempt();
    Set<String> steps = message.getPipelineSteps();

    // Main message processing logic, converts an ABCD archive to an avro file.
    Runnable runnable = () -> {
      ArchiveToAvroPath paths = PathFactory.create(XML).from(config, datasetId, attempt);
      XmlToAvroConverter.create()
        .xmlReaderParallelism(config.xmlReaderParallelism)
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
      .zkRootElementPath(XML_TO_VERBATIM)
      .pipelinesStepName(Steps.XML_TO_VERBATIM.name())
      .publisher(publisher)
      .runnable(runnable)
      .build()
      .handleMessage();

  }
}
