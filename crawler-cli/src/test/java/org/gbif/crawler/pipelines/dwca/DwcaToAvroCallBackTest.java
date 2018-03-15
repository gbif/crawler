package org.gbif.crawler.pipelines.dwca;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
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
public class DwcaToAvroCallBackTest {


  private static final String DATASET_UUID_POS = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DUMMY_URL = "http://some.new.url";
  private static final String INPUT_DATASET_FOLDER_POS = "dataset";
  private static final Configuration CONFIG = new Configuration();
  private static String hdfsUri;
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    File baseDir = new File("minicluster").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    CONFIG.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(CONFIG);
    cluster = builder.build();
    hdfsUri = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    cluster.waitClusterUp();
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testPositiveCase() throws IOException {
    DwCAToAvroConfiguration config = new DwCAToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER_POS;
    config.extendedRecordRepository = hdfsUri;
    DwCAToAvroCallBack callback = new DwCAToAvroCallBack(config);

    DwcaValidationFinishedMessage finishedMessage = new DwcaValidationFinishedMessage(UUID.fromString(DATASET_UUID_POS),
                                                                                      DatasetType.OCCURRENCE,
                                                                                      URI.create(DUMMY_URL),
                                                                                      2,
                                                                                      new DwcaValidationReport(UUID.fromString(
                                                                                        DATASET_UUID_POS),
                                                                                                               "no reason"));
    callback.handleMessage(finishedMessage);
    Assert.assertTrue(cluster.getFileSystem()
                        .exists(new Path(hdfsUri  + DATASET_UUID_POS + "/2_verbatim.avro")));
    Assert.assertTrue(cluster.getFileSystem()
                        .getStatus(new Path(hdfsUri + DATASET_UUID_POS + "/2_verbatim.avro"))
                        .getCapacity() > 0);
  }

}
