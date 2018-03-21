package org.gbif.crawler.pipelines.path;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import com.google.common.base.Preconditions;

class XmlToAvroPath extends ArchiveToAvroPath {

  /**
   * input path result example, directory - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2,
   * if directory is absent, tries get a tar archive  - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
   */
  @Override
  Path buildInputPath(String archiveRepository, UUID dataSetUuid, int attempt){
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString(), String.valueOf(attempt));
    if (!directoryPath.toFile().exists()) {
      String filePath = directoryPath.toString() + ".tar.xz";
      Path archivePath = Paths.get(filePath);
      Preconditions.checkState(archivePath.toFile().exists(), "Directory - %s or archive %s does not exist!", directoryPath, archivePath);
    }
    return directoryPath;
  }

}
