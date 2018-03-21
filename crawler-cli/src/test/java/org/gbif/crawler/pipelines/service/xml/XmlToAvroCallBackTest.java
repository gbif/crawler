package org.gbif.crawler.pipelines.service.xml;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;
import org.gbif.crawler.pipelines.ConverterConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class XmlToAvroCallBackTest {

  private static final String AVRO = "/verbatim.avro";

  private static final Configuration CONFIG = new Configuration();
  private static String hdfsUri;
  private static MiniDFSCluster cluster;
  private static FileSystem clusterFS;

  @BeforeClass
  public static void setUp() throws IOException {
    File baseDir = new File("minicluster").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    CONFIG.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(CONFIG);
    cluster = builder.build();
    hdfsUri = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    cluster.waitClusterUp();
    clusterFS = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testXmlDirectory() throws IOException {

    // When
    String uuid = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
    String inputFolder = "dataset/xml";
    int attempt = 61;

    // Expected
    ConverterConfiguration config = new ConverterConfiguration();
    config.archiveRepository = inputFolder;
    config.extendedRecordRepository = hdfsUri;
    XmlToAvroCallBack callback = new XmlToAvroCallBack(config);

    CrawlFinishedMessage message = new CrawlFinishedMessage(UUID.fromString(uuid), attempt, 20, FinishReason.NORMAL);
    callback.handleMessage(message);

    // Should
    assertTrue(cluster.getFileSystem().exists(new Path(hdfsUri + uuid + "/" + attempt + AVRO)));
    assertTrue(cluster.getFileSystem().getStatus(new Path(hdfsUri + uuid + "/" + attempt + AVRO)).getCapacity() > 0);
  }

  @Test
  public void testXmlTarArchive() throws IOException {

    // When
    String uuid = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
    String inputFolder = "dataset/xml";
    int attempt = 60;

    // Expected
    ConverterConfiguration config = new ConverterConfiguration();
    config.archiveRepository = inputFolder;
    config.extendedRecordRepository = hdfsUri;
    XmlToAvroCallBack callback = new XmlToAvroCallBack(config);

    CrawlFinishedMessage message = new CrawlFinishedMessage(UUID.fromString(uuid), attempt, 20, FinishReason.NORMAL);
    callback.handleMessage(message);

    // Should
    assertTrue(cluster.getFileSystem().exists(new Path(hdfsUri + uuid + "/" + attempt + AVRO)));
    assertTrue(cluster.getFileSystem().getStatus(new Path(hdfsUri + uuid + "/" + attempt + AVRO)).getCapacity() > 0);
  }

}