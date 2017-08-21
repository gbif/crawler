package org.gbif.crawler.dwca.util;

import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

public class DwcaTestUtil {

  /**
   * Copies a zip archive or a single file from the test resources into a random uuid folder and returns the opened archive.
   */
  public static Archive openArchive(String archiveResourcePath) throws IOException {
    UUID uuid = UUID.randomUUID();

    File srcFile = new File(DwcaTestUtil.class.getResource(archiveResourcePath).getFile());
    File tmpFile = new File(srcFile.getParentFile(), uuid.toString() + ".dwca");
    Files.copy(srcFile, tmpFile);

    File dwcaDir = new File(tmpFile.getParent(), uuid.toString());
    if("zip".equalsIgnoreCase(FilenameUtils.getExtension(srcFile.getName()))) {
      return ArchiveFactory.openArchive(tmpFile, dwcaDir);
    }

    return ArchiveFactory.openArchive(tmpFile);

  }

  public static void cleanupArchive(Archive archive) {
    File zip = archive.getLocation();
    FileUtils.deleteQuietly(zip);
  }

}
