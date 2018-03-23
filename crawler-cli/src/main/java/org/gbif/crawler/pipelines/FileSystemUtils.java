package org.gbif.crawler.pipelines;

import org.gbif.crawler.pipelines.service.xml.XmlToAvroService;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);

  private FileSystemUtils() {
  }

  /**
   * Helper method to get file system based on provided configuration.
   */
  public static FileSystem getFileSystem(String extendedRecordRepository, String hdfsSiteConfig) {
    try {
      Configuration config = new Configuration();

      // check if the hdfs-site.xml is provided
      if (!Strings.isNullOrEmpty(hdfsSiteConfig)) {
        File hdfsSite = new File(hdfsSiteConfig);
        if (hdfsSite.exists() && hdfsSite.isFile()) {
          LOG.info("using hdfs-site.xml");
          config.addResource(hdfsSite.toURI().toURL());
        } else {
          LOG.warn("hdfs-site.xml does not exist");
        }
      }

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
  public static FileSystem createParentDirectories(Path extendedRepoPath, String hdfsSite) throws IOException {
    FileSystem fs = getFileSystem(extendedRepoPath.toString(), hdfsSite);
    fs.mkdirs(extendedRepoPath.getParent());
    return fs;
  }
}
