package org.gbif.crawler.pipelines;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemUtils {

  private FileSystemUtils() {
  }

  /**
   * Helper method to get file system based on provided configuration.
   */
  public static FileSystem getFileSystem(String extendedRecordRepository) {
    try {
      Configuration config = new Configuration();
      return FileSystem.get(URI.create(extendedRecordRepository), config);
    } catch (IOException ex) {
      throw new IllegalStateException("Can't get a valid filesystem from provided uri " + extendedRecordRepository, ex);
    }
  }

  /**
   * Helper method to create a parent directory in the provided path
   *
   * @return filesystem
   */
  public static FileSystem createParentDirectories(Path extendedRepoPath) throws IOException {
    FileSystem fs = getFileSystem(extendedRepoPath.toString());
    fs.mkdirs(extendedRepoPath.getParent());
    return fs;
  }
}
