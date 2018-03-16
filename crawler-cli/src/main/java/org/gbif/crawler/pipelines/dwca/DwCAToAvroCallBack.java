package org.gbif.crawler.pipelines.dwca;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.pipelines.core.io.DwCAReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } is received.
 */
public class DwCAToAvroCallBack extends AbstractMessageCallback<DwcaValidationFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwCAToAvroCallBack.class);
  private final DwCAToAvroConfiguration configuration;

  DwCAToAvroCallBack(DwCAToAvroConfiguration configuration) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
  }

  @Override
  public void handleMessage(DwcaValidationFinishedMessage dwcaValidationFinishedMessage) {
    LOG.info("Received Download finished validation message {}", dwcaValidationFinishedMessage);
    LOG.info("Verifying the configuration parameters for dataset {}, before exporting",
             dwcaValidationFinishedMessage.getDatasetUuid());
    DwCAToAvroPaths paths = DwCAToAvroPaths.from(configuration, dwcaValidationFinishedMessage);

    DatumWriter<ExtendedRecord> writer = new SpecificDatumWriter<>();
    try (FileSystem fs = createParentDirectories(paths.getExtendedRepositoryExportPath());
         OutputStream extendedRepoPath = fs.create(paths.getExtendedRepositoryExportPath());
         DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(writer)
           .create(ExtendedRecord.getClassSchema(), extendedRepoPath)) {

      LOG.info("Extracting the DwC Archive {} started", paths.getDwcaExpandedPath());
      DwCAReader reader =
        new DwCAReader(paths.getDwcaExpandedPath().toString());
      reader.init();
      LOG.info("Exporting the DwC Archive to avro {} started", paths.getExtendedRepositoryExportPath());
      while (reader.advance()) {
        dataFileWriter.append(reader.getCurrent());
      }

    } catch (IOException e) {
      LOG.error("Failed performing conversion on "+ dwcaValidationFinishedMessage.getDatasetUuid(), e);
      throw new IllegalStateException("Failed performing conversion on " + dwcaValidationFinishedMessage.getDatasetUuid(),
                                 e);
    }
    LOG.info("DwCA to avro conversion completed for {}", dwcaValidationFinishedMessage.getDatasetUuid());
  }

  /**
   * Helper method to get file system based on provided configuration
   */
  private FileSystem getFileSystem() {
    try {
      return FileSystem.get(URI.create(configuration.extendedRecordRepository), new Configuration());
    } catch (IOException ex) {
      throw new IllegalStateException("Cannot get a valid filesystem from provided uri "
                                      + configuration.extendedRecordRepository, ex);
    }
  }

  private FileSystem createParentDirectories(Path extendedRepoPath) throws IOException {
    FileSystem fs = getFileSystem();
    fs.mkdirs(extendedRepoPath);
    return fs;
  }

}
