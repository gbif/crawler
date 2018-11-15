package org.gbif.crawler.pipelines.service.dwca;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.DWCA;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } is received.
 */
public class DwcaToAvroCallback extends AbstractMessageCallback<DwcaValidationFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToAvroCallback.class);
  private final ConverterConfiguration configuration;
  private final MessagePublisher publisher;

  DwcaToAvroCallback(ConverterConfiguration configuration, MessagePublisher publisher) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
    this.publisher = publisher;
  }

  @Override
  public void handleMessage(DwcaValidationFinishedMessage message) {
    LOG.info("Received Download finished validation message {}", message);

    UUID datasetUuid = message.getDatasetUuid();

    ArchiveToAvroPath paths = PathFactory.create(DWCA).from(configuration, datasetUuid, message.getAttempt());

    boolean isFileCreated = DwcaToAvroConverter.create()
      .codecFactory(configuration.avroConfig.getCodec())
      .syncInterval(configuration.avroConfig.syncInterval)
      .hdfsSiteConfig(configuration.hdfsSiteConfig)
      .convert(paths.getInputPath(), paths.getOutputPath());

    LOG.info("DWCA to avro conversion completed for {}, file was created - {}", datasetUuid, isFileCreated);

    // Send message to MQ
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
