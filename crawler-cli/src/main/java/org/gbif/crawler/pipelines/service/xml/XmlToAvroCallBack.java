package org.gbif.crawler.pipelines.service.xml;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.converter.Xml2Verbatim;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.XML;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.CrawlFinishedMessage } is received.
 */
public class XmlToAvroCallBack extends AbstractMessageCallback<CrawlFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroCallBack.class);
  private final ConverterConfiguration configuration;
  private final MessagePublisher publisher;

  public XmlToAvroCallBack(ConverterConfiguration configuration, MessagePublisher publisher) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
    this.publisher = publisher;
  }

  @Override
  public void handleMessage(CrawlFinishedMessage message) {
    LOG.info("Received Download finished validation message {}", message);

    UUID datasetUuid = message.getDatasetUuid();

    ArchiveToAvroPath paths = PathFactory.create(XML).from(configuration, datasetUuid, message.getAttempt());

    boolean isFileCreated = Xml2Verbatim.create()
      .xmlReaderParallelism(configuration.xmlReaderParallelism)
      .codecFactory(configuration.avroConfig.getCodec())
      .syncInterval(configuration.avroConfig.syncInterval)
      .hdfsSiteConfig(configuration.hdfsSiteConfig)
      .convert(paths.getInputPath(), paths.getOutputPath());

    LOG.info("XML to avro conversion completed for {}, file was created - {}", datasetUuid, isFileCreated);

    if (isFileCreated && Objects.nonNull(publisher)) {
      try {
        URI uri = paths.getOutputPath().toUri();
        publisher.send(new ExtendedRecordAvailableMessage(datasetUuid, message.getAttempt(), uri, configuration.interpretTypes));
        LOG.info("Message has been sent - {}", uri);
      } catch (IOException e) {
        LOG.error("Could not send message for dataset [{}] : {}", datasetUuid, e.getMessage());
      }
    }
  }
}
