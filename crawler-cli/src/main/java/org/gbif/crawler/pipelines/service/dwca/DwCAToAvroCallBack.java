package org.gbif.crawler.pipelines.service.dwca;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;
import org.gbif.crawler.pipelines.FileSystemUtils;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;
import org.gbif.pipelines.core.io.DwCAReader;
import org.gbif.pipelines.core.utils.DataFileWriteBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.DWCA;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } is received.
 */
public class DwCAToAvroCallBack extends AbstractMessageCallback<DwcaValidationFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwCAToAvroCallBack.class);
  private final ConverterConfiguration configuration;
  private final MessagePublisher publisher;

  DwCAToAvroCallBack(ConverterConfiguration configuration, MessagePublisher publisher) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
    this.publisher = publisher;
  }

  @Override
  public void handleMessage(DwcaValidationFinishedMessage message) {
    LOG.info("Received Download finished validation message {}", message);
    ArchiveToAvroPath paths =
      PathFactory.create(DWCA).from(configuration, message.getDatasetUuid(), message.getAttempt());

    // the fs has to be out of the try-catch block to avoid closing it, because the hdfs client tries to reuse the
    // same connection. So, when using multiple consumers, one consumer would close the connection that is being used
    // by another consumer.
    FileSystem fs = FileSystemUtils.createParentDirectories(paths.getOutputPath(), configuration.hdfsSiteConfig);
    try (BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(paths.getOutputPath()));
         DataFileWriter<ExtendedRecord> dataFileWriter = DataFileWriteBuilder.create()
           .schema(ExtendedRecord.getClassSchema())
           .codec(configuration.avroConfig.getCodec())
           .outputStream(outputStream)
           .syncInterval(configuration.avroConfig.syncInterval)
           .build()) {

      DwCAReader reader = new DwCAReader(paths.getInputPath().toString());
      reader.init();
      LOG.info("Exporting the DwC Archive to avro {} started", paths.getOutputPath());
      while (reader.advance()) {
        dataFileWriter.append(reader.getCurrent());
      }

    } catch (IOException e) {
      LOG.error("Failed performing conversion on {}", message.getDatasetUuid(), e);
      throw new IllegalStateException("Failed performing conversion on " + message.getDatasetUuid(), e);
    } finally {
      FileSystemUtils.deleteAvroFileIfEmpty(fs, paths.getOutputPath());
    }

    LOG.info("DwCA to avro conversion completed for {}", message.getDatasetUuid());
    try {
      if (publisher != null) {
        publisher.send(new ExtendedRecordAvailableMessage(message.getDatasetUuid(), paths.getOutputPath().toUri()));
      }
    } catch (IOException e) {
      LOG.error("Could not send message for dataset [{}] : {}", message.getDatasetUuid(), e.getMessage());
    }
  }

}
