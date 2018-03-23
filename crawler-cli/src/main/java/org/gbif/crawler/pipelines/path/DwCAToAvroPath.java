package org.gbif.crawler.pipelines.path;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import com.google.common.base.Preconditions;

class DwCAToAvroPath extends ArchiveToAvroPath {

  /**
   * input path example - /mnt/auto/crawler/dwca/9bed66b3-4caa-42bb-9c93-71d7ba109dad
   */
  @Override
  Path buildInputPath(String archiveRepository, UUID dataSetUuid, int attempt) {
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString());
    Preconditions.checkState(directoryPath.toFile().exists(), "Directory - %s does not exist!", directoryPath);

    return directoryPath;
  }

}
