package org.gbif.crawler.pipelines.service.dwca;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.UUID;

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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test dwca-to-avro commands message handling command on hdfs
 */
public class DwcaToAvroCallbackTest {

  private static final String DATASET_UUID_POS = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DUMMY_URL = "http://some.new.url";
  private static final String INPUT_DATASET_FOLDER = "dataset/dwca";
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
  public void testPositiveCase() throws IOException {
    // When
    ConverterConfiguration config = new ConverterConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    DwcaToAvroCallback callback = new DwcaToAvroCallback(config, null, curator);
    UUID uuid = UUID.fromString(DATASET_UUID_POS);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, "no reason");
    PipelinesDwcaMessage message =
      new PipelinesDwcaMessage(uuid, DatasetType.OCCURRENCE, URI.create(DUMMY_URL), 2, reason, Collections.singleton("ALL"));

    // Expected
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + DATASET_UUID_POS + "/2/verbatim.avro");
    Assert.assertTrue(cluster.getFileSystem().exists(path));
    Assert.assertTrue(clusterFs.getFileStatus(path).getLen() > 0);
  }

}
