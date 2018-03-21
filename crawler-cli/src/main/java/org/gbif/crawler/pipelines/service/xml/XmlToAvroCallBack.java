package org.gbif.crawler.pipelines.service.xml;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;
import org.gbif.crawler.pipelines.ConverterConfiguration;
import org.gbif.crawler.pipelines.FileSystemUtils;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;
import org.gbif.xml.occurrence.parser.ExtendedRecordParser;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.XML;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.CrawlFinishedMessage } is received.
 */
public class XmlToAvroCallBack extends AbstractMessageCallback<CrawlFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroService.class);

  private final ConverterConfiguration configuration;

  public XmlToAvroCallBack(ConverterConfiguration configuration) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
  }

  @Override
  public void handleMessage(CrawlFinishedMessage message) {

    LOG.info("Received Download finished validation message {}", message);
    ArchiveToAvroPath paths = PathFactory.create(XML).from(configuration, message.getDatasetUuid(), message.getAttempt());

    try (FileSystem fs = FileSystemUtils.createParentDirectories(paths.getOutputPath());
         BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(paths.getOutputPath()))) {

      LOG.info("Parsing process has been started");
      ExtendedRecordParser.convertFromXML(paths.getInputPath().toString(), outputStream);
      LOG.info("Parsing process has been finished");

    } catch (IOException ex) {
      LOG.error("Failed performing conversion on {}", message.getDatasetUuid(), ex);
      throw new IllegalStateException("Failed performing conversion on " + message.getDatasetUuid(), ex);
    }
    LOG.info("DwCA to avro conversion completed for {}", message.getDatasetUuid());

  }
}
