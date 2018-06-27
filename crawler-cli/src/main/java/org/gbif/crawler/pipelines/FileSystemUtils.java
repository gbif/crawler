package org.gbif.crawler.pipelines;

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

  public static long fileSize(URI file, String hdfsSiteConfig) throws IOException {
    FileSystem fs = getFileSystem(file.toString(), hdfsSiteConfig);
    return fs.getFileStatus(new Path(file)).getLen();
  }

}
