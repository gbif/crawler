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
  private static final String STRING_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final UUID DATASET_UUID = UUID.fromString(STRING_UUID);
  private static final String INPUT_DATASET_FOLDER = "dataset/xml";

  private static final Configuration CONFIG = new Configuration();
  private static String hdfsUri;
  private static MiniDFSCluster cluster;
  private static FileSystem clusterFs;

  @BeforeClass
  public static void setUp() throws IOException {
    File baseDir = new File("minicluster").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    CONFIG.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(CONFIG);
    cluster = builder.build();
    hdfsUri = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    cluster.waitClusterUp();
    clusterFs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    clusterFs.close();
    cluster.shutdown();
  }

  @Test
  public void testXmlDirectory() throws IOException {
    // When
    int attempt = 61;
    ConverterConfiguration config = new ConverterConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.extendedRecordRepository = hdfsUri;
    config.xmlReaderParallelism = 4;
    XmlToAvroCallBack callback = new XmlToAvroCallBack(config);
    CrawlFinishedMessage message = new CrawlFinishedMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL);

    // Expected
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertTrue(cluster.getFileSystem().exists(path));
    assertTrue(clusterFs.getFileStatus(path).getLen() > 0);
  }

  @Test
  public void testXmlTarArchive() throws IOException {
    // When
    int attempt = 60;
    ConverterConfiguration config = new ConverterConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.extendedRecordRepository = hdfsUri;
    config.xmlReaderParallelism = 4;
    XmlToAvroCallBack callback = new XmlToAvroCallBack(config);
    CrawlFinishedMessage message = new CrawlFinishedMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL);

    // Expected
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertTrue(cluster.getFileSystem().exists(path));
    assertTrue(clusterFs.getFileStatus(path).getLen() > 0);
  }

  @Test
  public void testXmlEmptyAvro() throws IOException {
    // When
    int attempt = 62;
    ConverterConfiguration config = new ConverterConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.extendedRecordRepository = hdfsUri;
    config.xmlReaderParallelism = 4;
    XmlToAvroCallBack callback = new XmlToAvroCallBack(config);
    CrawlFinishedMessage message = new CrawlFinishedMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL);

    // Expected
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertFalse(cluster.getFileSystem().exists(path));
  }

}