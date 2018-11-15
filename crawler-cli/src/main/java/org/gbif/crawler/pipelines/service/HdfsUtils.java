package org.gbif.crawler.pipelines.service;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);

  private HdfsUtils() {
    // NOP
  }

  /**
   * Returns the file size in bytes
   *
   * @param filePath       path to some file
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   */
  public static long getfileSizeByte(String filePath, String hdfsSiteConfig) throws IOException {
    URI fileUri = URI.create(filePath);
    FileSystem fs = getFileSystem(fileUri, hdfsSiteConfig);
    return fs.getFileStatus(new Path(fileUri)).getLen();
  }

  /**
   * Returns number of files in the directory
   *
   * @param directoryPath  path to some directory
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   */
  public static int getfileCount(String directoryPath, String hdfsSiteConfig) throws IOException {
    URI fileUri = URI.create(directoryPath);
    FileSystem fs = getFileSystem(fileUri, hdfsSiteConfig);

    int count = 0;
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(fileUri), false);
    while (iterator.hasNext()) {
      LocatedFileStatus fileStatus = iterator.next();
      if (fileStatus.isFile()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Gets HDFS file system using config file or without if it doesn't exist
   *
   * @param filePath       path to some file
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   */
  private static FileSystem getFileSystem(URI filePath, String hdfsSiteConfig) {
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

      return FileSystem.get(filePath, config);
    } catch (IOException ex) {
      throw new IllegalStateException("Can't get a valid filesystem from provided uri " + filePath, ex);
    }
  }
}
