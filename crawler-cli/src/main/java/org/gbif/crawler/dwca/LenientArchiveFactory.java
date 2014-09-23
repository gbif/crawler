package org.gbif.crawler.dwca;

import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility that wraps up the ArchiveFactory, but addresses a critical IPT bug with a workaround.
 * https://code.google.com/p/gbif-providertoolkit/issues/detail?id=1015
 */
public class LenientArchiveFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LenientArchiveFactory.class);
  private static final Pattern DOUBLE_BACKSLASH = Pattern.compile("\\\\");

  public static Archive openArchive(File unzippedFolderLocation) throws IOException, UnsupportedArchiveException {
    checkNotNull(unzippedFolderLocation, "unzippedFolderLocation can not be null");

    for (File f : unzippedFolderLocation.listFiles()) {
      if (f.getName().startsWith("\\")) {
        String cleanedName = DOUBLE_BACKSLASH.matcher(f.getName()).replaceFirst("");
        LOG.info("Handling IPT windows bug[code issue #1015], renaming {} to {}", f.getName(), cleanedName);
        if (!f.renameTo(new File(unzippedFolderLocation, cleanedName))) {
          LOG.warn("Failed to rename file with backslash, archive {} probably won't work as expected", f.getName());
        }
      }
    }
    return ArchiveFactory.openArchive(unzippedFolderLocation);
  }

  /**
   * See {@link ArchiveFactory}
   */
  public static Archive openArchive(File archiveFile, File archiveDir) throws IOException, UnsupportedArchiveException {
    try {
      if (archiveDir.exists()) {
        // clean up any existing folder
        LOG.debug("Deleting existing archive folder [{}]", archiveDir.getAbsolutePath());
        FileUtils.deleteDirectoryRecursively(archiveDir);
      }
      org.apache.commons.io.FileUtils.forceMkdir(archiveDir);

      CompressionUtil.decompressFile(archiveDir, archiveFile);
      return openArchive(archiveDir);

    } catch (CompressionUtil.UnsupportedCompressionType e) {
      LOG.debug("Could not uncompress archive [{}], try to read as single text file", archiveFile, e);

      Archive archive = ArchiveFactory.openArchive(archiveFile);
      LOG.debug("Was able to read plain text file for archive {}", archiveFile);
      Files.copy(archiveFile, new File(archiveDir, Files.getNameWithoutExtension(archiveFile.getName()) + ".txt"));
      return archive;
    }
  }
}
