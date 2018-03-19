package org.gbif.crawler.pipelines.dwca;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.pipelines.core.io.DwCAReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import org.apache.avro.file.DataFileWriter;
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
    DwCAToAvroPaths paths = DwCAToAvroPaths.from(configuration, dwcaValidationFinishedMessage);

    try (FileSystem fs = createParentDirectories(paths.getExtendedRepositoryExportPath());
         BufferedOutputStream extendedRepoPath = new BufferedOutputStream(fs.create(paths.getExtendedRepositoryExportPath()));
         DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<ExtendedRecord>())
           .create(ExtendedRecord.getClassSchema(), extendedRepoPath)) {

      LOG.info("Extracting the DwC Archive {} started", paths.getDwcaExpandedPath());
      DwCAReader reader = new DwCAReader(paths.getDwcaExpandedPath().toString());
      reader.init();
      LOG.info("Exporting the DwC Archive to avro {} started", paths.getExtendedRepositoryExportPath());
      while (reader.advance()) {
        dataFileWriter.append(reader.getCurrent());
      }

    } catch (IOException e) {
      LOG.error("Failed performing conversion on {}", dwcaValidationFinishedMessage.getDatasetUuid(), e);
      throw new IllegalStateException("Failed performing conversion on "
                                      + dwcaValidationFinishedMessage.getDatasetUuid(), e);
    }
    LOG.info("DwCA to avro conversion completed for {}", dwcaValidationFinishedMessage.getDatasetUuid());
  }

  /**
   * Helper method to get file system based on provided configuration
   */
  private FileSystem getFileSystem() {
    try {
      Configuration config = new Configuration();
      //aded to avoid IOException : No FileSystem for scheme: hdfs
      config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      return FileSystem.get(URI.create(configuration.extendedRecordRepository), config);
    } catch (IOException ex) {
      throw new IllegalStateException("Cannot get a valid filesystem from provided uri "
                                      + configuration.extendedRecordRepository, ex);
    }
  }

  private FileSystem createParentDirectories(Path extendedRepoPath) throws IOException {
    FileSystem fs = getFileSystem();
    fs.mkdirs(extendedRepoPath.getParent());
    return fs;
  }

}
