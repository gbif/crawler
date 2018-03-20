package org.gbif.crawler.pipelines.dwca;

import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import java.nio.file.Paths;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

/**
 * Contains derived paths for input(dwcaExpandedPath) and output(extendedRecordsRepository)
 */
class DwCAToAvroPaths {

  private final Path extendedRepositoryExportPath;
  private final java.nio.file.Path dwcaExpandedPath;

  /**
   * Derives expanded DwCA path and target avro files path from configuration and received message.
   * @param configuration
   * @param receivedMessage
   * @return derived input,output paths
   */
  public static DwCAToAvroPaths from(DwCAToAvroConfiguration configuration, DwcaValidationFinishedMessage receivedMessage) {
    //calculates and checks existence of DwC Archive
    java.nio.file.Path dwcaExpandedPath =
      Paths.get(configuration.archiveRepository, receivedMessage.getDatasetUuid().toString());
    Preconditions.checkState(dwcaExpandedPath.toFile().exists(), "Could not find %s not available", dwcaExpandedPath);
    //calculates export path of avro as extended record
    String extendedRecordRepositoryPath = configuration.extendedRecordRepository.endsWith(Path.SEPARATOR)
      ? configuration.extendedRecordRepository
      : configuration.extendedRecordRepository + Path.SEPARATOR;
    Path extendedRepositoryExportPath = new Path(extendedRecordRepositoryPath
                                                 + Path.SEPARATOR
                                                 + receivedMessage.getDatasetUuid()
                                                 + Path.SEPARATOR
                                                 + receivedMessage.getAttempt()
                                                 + "_verbatim.avro");

    return new DwCAToAvroPaths(dwcaExpandedPath, extendedRepositoryExportPath);
  }

  private DwCAToAvroPaths(java.nio.file.Path dwcaExpandedPath, Path extendedRepositoryExportPath) {
    this.extendedRepositoryExportPath = extendedRepositoryExportPath;
    this.dwcaExpandedPath = dwcaExpandedPath;
  }

  /**
   * Target path where the avro files in {@link org.gbif.pipelines.io.avro.ExtendedRecord} format is written.
   * @return extendedRepository export path
   */
  public Path getExtendedRepositoryExportPath() {
    return extendedRepositoryExportPath;
  }

  /**
   * Source Path of the expanded DwC Archive, required for reading the dataset.
   * @return expanded DwCA dataset path
   */
  public java.nio.file.Path getDwcaExpandedPath() {
    return dwcaExpandedPath;
  }
}
