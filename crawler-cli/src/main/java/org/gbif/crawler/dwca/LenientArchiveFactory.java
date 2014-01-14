package org.gbif.crawler.dwca;

import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.file.DownloadUtil;
import org.gbif.utils.file.CompressionUtil;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.regex.Pattern;

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
      CompressionUtil.decompressFile(archiveDir, archiveFile);
      return openArchive(archiveDir);

    } catch (CompressionUtil.UnsupportedCompressionType e) {
      return openArchive(archiveFile);
    }
  }

  /**
   * See {@link ArchiveFactory}
   */
  public static Archive openArchive(URL archiveUrl, File workingDir) throws IOException, UnsupportedArchiveException {
    File downloadTo = new File(workingDir, "dwca-download");
    File dwca = new File(workingDir, "dwca");
    DownloadUtil.download(archiveUrl, downloadTo);
    return openArchive(downloadTo, dwca);
  }
}
