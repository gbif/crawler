package org.gbif.crawler.pipelines;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
  public static long getFileSizeByte(String filePath, String hdfsSiteConfig) throws IOException {
    URI fileUri = URI.create(filePath);
    FileSystem fs = getFileSystem(fileUri, hdfsSiteConfig);
    Path path = new Path(fileUri);

    return fs.exists(path) ? fs.getContentSummary(path).getLength() : -1;
  }

  /**
   * Returns number of files in the directory
   *
   * @param directoryPath path to some directory
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   */
  public static int getFileCount(String directoryPath, String hdfsSiteConfig) throws IOException {
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
   * Checks directory
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param filePath to directory
   */
  public static boolean exists(String hdfsSiteConfig, String filePath) throws IOException {
    FileSystem fs = getFileSystem(URI.create(filePath), hdfsSiteConfig);
    Path fsPath = new Path(filePath);
    return fs.exists(fsPath);
  }

  /**
   * Returns sub directory list
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param filePath to directory
   */
  public static List<String> getSubDirList(String hdfsSiteConfig, String filePath) throws IOException {
    FileSystem fs = getFileSystem(URI.create(filePath), hdfsSiteConfig);
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      FileStatus[] statuses = fs.listStatus(fsPath);
      if (statuses != null && statuses.length > 0) {
        return Arrays.stream(statuses)
            .filter(FileStatus::isDirectory)
            .map(y -> y.getPath().getName())
            .collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
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
   * Delete HDFS directory
   */
  public static boolean deleteDirectory(String hdfsSiteConfig, String filePath) {
    FileSystem fs = getFileSystem(URI.create(filePath), hdfsSiteConfig);
    Path fsPath = new Path(filePath);
    try {
      if (fs.exists(fsPath)) {
        return fs.delete(fsPath, true);
      }
    } catch (IOException ex) {
      throw new IllegalStateException("Exception during deletion " + filePath, ex);
    }

    return true;
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