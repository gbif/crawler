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

  public static DwCAToAvroPaths from(
    DwCAToAvroConfiguration configuration, DwcaValidationFinishedMessage receivedMessage
  ) {
    //calculates and checks existence of DwC Archive
    java.nio.file.Path dwcaExpandedPath =
      Paths.get(configuration.archiveRepository, receivedMessage.getDatasetUuid().toString());
    Preconditions.checkState(dwcaExpandedPath.toFile().exists(), "Could not find %s not available", dwcaExpandedPath);
    //calculates export path of avro as extended record
    Path extendedRepositoryExportPath = new Path(Paths.get(configuration.extendedRecordRepository,
                                                           receivedMessage.getDatasetUuid().toString(),
                                                           receivedMessage.getAttempt() + "_verbatim.avro").toUri());
    return new DwCAToAvroPaths(dwcaExpandedPath, extendedRepositoryExportPath);
  }

  private DwCAToAvroPaths(java.nio.file.Path dwcaExpandedPath, Path extendedRepositoryExportPath) {
    this.extendedRepositoryExportPath = extendedRepositoryExportPath;
    this.dwcaExpandedPath = dwcaExpandedPath;
  }

  public Path getExtendedRepositoryExportPath() {
    return extendedRepositoryExportPath;
  }

  public java.nio.file.Path getDwcaExpandedPath() {
    return dwcaExpandedPath;
  }
}
