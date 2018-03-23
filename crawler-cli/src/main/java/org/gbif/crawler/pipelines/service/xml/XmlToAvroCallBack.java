package org.gbif.crawler.pipelines.service.xml;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;
import org.gbif.crawler.pipelines.ConverterConfiguration;
import org.gbif.crawler.pipelines.FileSystemUtils;
import org.gbif.crawler.pipelines.path.ArchiveToAvroPath;
import org.gbif.crawler.pipelines.path.PathFactory;
import org.gbif.pipelines.core.utils.AvroUtil;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.xml.occurrence.parser.ParsingException;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ConverterTask;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ParserFileUtils;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.SyncDataFileWriter;
import org.gbif.xml.occurrence.parser.parsing.validators.UniquenessValidator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.XML;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.CrawlFinishedMessage } is received.
 */
public class XmlToAvroCallBack extends AbstractMessageCallback<CrawlFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroService.class);
  private static final String FILE_PREFIX = ".response";
  private final ConverterConfiguration configuration;

  public XmlToAvroCallBack(ConverterConfiguration configuration) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
  }

  @Override
  public void handleMessage(CrawlFinishedMessage message) {

    LOG.info("Received Download finished validation message {}", message);
    ArchiveToAvroPath paths =
      PathFactory.create(XML).from(configuration, message.getDatasetUuid(), message.getAttempt());

    try (FileSystem fs = FileSystemUtils.createParentDirectories(paths.getOutputPath(), configuration.hdfsSiteConfig);
         BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(paths.getOutputPath()))) {

      LOG.info("Parsing process has been started");

      convertFromXML(paths.getInputPath().toString(),
                     outputStream,
                     configuration.avroConfig.syncInterval,
                     configuration.avroConfig.getCodec());
      LOG.info("Parsing process has been finished");

    } catch (IOException ex) {
      LOG.error("Failed performing conversion on {}", message.getDatasetUuid(), ex);
      throw new IllegalStateException("Failed performing conversion on " + message.getDatasetUuid(), ex);
    }
    LOG.info("XML to avro conversion completed for {}", message.getDatasetUuid());

  }

  /**
   * @param inputPath    path to directory with response files or a tar.xz archive
   * @param outputStream output stream to support any file system
   */
  private void convertFromXML(String inputPath, OutputStream outputStream, int syncInterval, CodecFactory codec) {

    if (Strings.isNullOrEmpty(inputPath) || Objects.isNull(outputStream)) {
      throw new ParsingException("Input or output stream must not be empty or null!");
    }

    File inputFile = ParserFileUtils.uncompressAndGetInputFile(inputPath);
    Schema schema = ExtendedRecord.getClassSchema();

    try (DataFileWriter<ExtendedRecord> dataFileWriter = AvroUtil.getExtendedRecordWriter(outputStream,
                                                                                          syncInterval,
                                                                                          codec);
         Stream<Path> walk = Files.walk(inputFile.toPath());
         UniquenessValidator validator = UniquenessValidator.getNewInstance()) {
      dataFileWriter.setFlushOnEveryBlock(false);

      // Class with sync method to avoid problem with writing
      SyncDataFileWriter syncWriter = new SyncDataFileWriter(dataFileWriter);

      // Run async process - read a file, convert to ExtendedRecord and write to avro
      CompletableFuture[] futures = walk.filter(x -> x.toFile().isFile() && x.toString().endsWith(FILE_PREFIX))
        .map(Path::toFile)
        .map(file -> CompletableFuture.runAsync(new ConverterTask(file, syncWriter, validator)))
        .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();
      dataFileWriter.flush();

    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new ParsingException(ex);
    }
  }
}
