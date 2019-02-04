package org.gbif.crawler.pipelines;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Utils help to work with HDFS files
 */
public class HdfsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);

  private HdfsUtils() {
    // NOP
  }

  /**
   * Returns the file size in bytes
   *
   * @param filePath path to some file
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   */
  public static long getfileSizeByte(String filePath, String hdfsSiteConfig) throws IOException {
    URI fileUri = URI.create(filePath);
    FileSystem fs = getFileSystem(fileUri, hdfsSiteConfig);
    Path path = new Path(fileUri);

    return fs.exists(path) ? fs.getFileStatus(path).getLen() : -1;
  }

  /**
   * Returns number of files in the directory
   *
   * @param directoryPath path to some directory
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
   * Removes a directory with content if the folder exists
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param directoryPath path to some directory
   */
  public static boolean deleteIfExist(String hdfsSiteConfig, String directoryPath) throws IOException {
    URI fileUri = URI.create(directoryPath);
    FileSystem fs = getFileSystem(fileUri, hdfsSiteConfig);

    Path path = new Path(directoryPath);
    return fs.exists(path) && fs.delete(path, true);
  }

  /**
   * Reads a yaml file and returns value by key
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param filePath to a yaml file
   * @param key to value in yaml
   */
  public static String getValueByKey(String hdfsSiteConfig, String filePath, String key) throws IOException {
    FileSystem fs = getFileSystem(URI.create(filePath), hdfsSiteConfig);
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fsPath)))) {
        return br.lines()
            .map(x -> x.replace("\u0000", ""))
            .filter(y -> y.startsWith(key))
            .findFirst()
            .map(z -> z.replace(key + ": ", ""))
            .orElse("");
      }
    }
    return "";
  }

  /**
   * Store an Avro file on HDFS in /data/ingest/<datasetUUID>/<attemptID>/verbatim.avro
   */
  public static Path buildOutputPath(String... values) {
    StringJoiner joiner = new StringJoiner(org.apache.hadoop.fs.Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new org.apache.hadoop.fs.Path(joiner.toString());
  }

  /**
   * Gets HDFS file system using config file or without if it doesn't exist
   *
   * @param filePath path to some file
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
