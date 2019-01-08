package org.gbif.crawler.pipelines.xml;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.gbif.crawler.pipelines.PipelineCallback.Steps.ALL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XmlToAvroCallbackTest {

  private static final String AVRO = "/verbatim.avro";
  private static final String STRING_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final UUID DATASET_UUID = UUID.fromString(STRING_UUID);
  private static final String INPUT_DATASET_FOLDER = "dataset/xml";

  private static final Configuration CONFIG = new Configuration();
  private static String hdfsUri;
  private static MiniDFSCluster cluster;
  private static FileSystem clusterFs;
  private static CuratorFramework curator;
  private static TestingServer server;

  @BeforeClass
  public static void setUp() throws Exception {
    File baseDir = new File("minicluster").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    CONFIG.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(CONFIG);
    cluster = builder.build();
    hdfsUri = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    cluster.waitClusterUp();
    clusterFs = cluster.getFileSystem();

    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder()
        .connectString(server.getConnectString())
        .namespace("crawler")
        .retryPolicy(new RetryOneTime(1))
        .build();
    curator.start();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    clusterFs.close();
    cluster.shutdown();
    curator.close();
    server.stop();
  }

  @Test
  public void testXmlDirectory() throws IOException {
    // When
    int attempt = 61;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    config.xmlReaderParallelism = 4;
    XmlToAvroCallback callback = new XmlToAvroCallback(config, null, curator);
    PipelinesXmlMessage message =
        new PipelinesXmlMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL, Collections.singleton(ALL.name()));

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
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    config.xmlReaderParallelism = 4;
    XmlToAvroCallback callback = new XmlToAvroCallback(config, null, curator);
    PipelinesXmlMessage message =
        new PipelinesXmlMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL, Collections.singleton(ALL.name()));

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
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    config.xmlReaderParallelism = 4;
    XmlToAvroCallback callback = new XmlToAvroCallback(config, null, curator);
    PipelinesXmlMessage message =
        new PipelinesXmlMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL, Collections.singleton(ALL.name()));

    // Expected
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertFalse(cluster.getFileSystem().exists(path));
    assertFalse(cluster.getFileSystem().exists(path.getParent()));
    // NOTE: If you run this method independently, it will fail, it is normal
    assertTrue(cluster.getFileSystem().exists(path.getParent().getParent()));
  }

}
