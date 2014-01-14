package org.gbif.crawler.dwca.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DwcaTestUtil {

  public static String openArchive(String archiveFilePath) throws IOException {
    String outputDir = UUID.randomUUID().toString();

    File zipFile = new File(DwcaTestUtil.class.getResource(archiveFilePath).getFile());
    FileInputStream fis = new FileInputStream(zipFile);
    ZipInputStream zis = new ZipInputStream(fis);
    ZipEntry ze = zis.getNextEntry();

    while (ze != null) {
      String fileName = ze.getName();
      File newFile = new File(zipFile.getParent(), outputDir + File.separator + fileName);

      new File(newFile.getParent()).mkdirs();
      FileOutputStream fos = new FileOutputStream(newFile);

      int len;
      byte[] buffer = new byte[1024];
      while ((len = zis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }

      fos.close();
      ze = zis.getNextEntry();
    }

    zis.closeEntry();
    zis.close();
    fis.close();

    return zipFile.getParent() + File.separator + outputDir;
  }

  public static void cleanupArchive(String dirName) {
    File openArchive = new File(dirName);
    for (File file : openArchive.listFiles()) {
      file.delete();
    }
  }
}
