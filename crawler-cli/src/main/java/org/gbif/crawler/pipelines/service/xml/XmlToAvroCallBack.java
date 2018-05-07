package org.gbif.crawler.pipelines.service.xml;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;
import org.gbif.crawler.pipelines.FileSystemUtils;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;
import org.gbif.pipelines.core.utils.DataFileWriteBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.xml.occurrence.parser.ExtendedRecordConverter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.fs.FileSystem;
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

    boolean isFileDeleted;

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

      LOG.info("Parsing process has been started");

      ExtendedRecordConverter.crete(configuration.xmlReaderParallelism)
        .toAvroFromXmlResponse(paths.getInputPath().toString(), dataFileWriter);

      LOG.info("Parsing process has been finished");

    } catch (IOException ex) {
      LOG.error("Failed performing conversion on {}", datasetUuid, ex);
      throw new IllegalStateException("Failed performing conversion on " + datasetUuid, ex);
    } finally {
      isFileDeleted = FileSystemUtils.deleteAvroFileIfEmpty(fs, paths.getOutputPath());
    }

    LOG.info("XML to avro conversion completed for {}, file was deleted - {}", datasetUuid, isFileDeleted);
    if (!isFileDeleted && Objects.nonNull(publisher)) {
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
