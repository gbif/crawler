package org.gbif.crawler.pipelines;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.Executors;

import org.gbif.crawler.constants.PipelinesNodePaths.Fn;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

import static org.gbif.crawler.constants.PipelinesNodePaths.ALL_STEPS;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

@RunWith(MockitoJUnitRunner.class)
public class PipelinesProcessServiceImplTest {

  private static final String MESSAGE = "info";
  private static final Boolean AVAILABILITY = Boolean.TRUE;

  private static CuratorFramework curator;
  private static TestingServer server;

  private final PipelinesProcessServiceImpl service;

  public PipelinesProcessServiceImplTest() {
    service = new PipelinesProcessServiceImpl(curator, Executors.newSingleThreadExecutor(), null, "test");
  }

  @BeforeClass
  public static void setUp() throws Exception {
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
    curator.close();
    server.stop();
  }

  @Test
  public void testEmptyGetRunningPipelinesProcesses() {
    // When
    Set<PipelinesProcessStatus> set = service.getRunningPipelinesProcesses();

    // Should
    Assert.assertEquals(0, set.size());
  }

  @Test
  public void testEmptyPipelinesProcessByCrawlId() {
    // State
    String crawlId = "a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b_1";

    // When
    PipelinesProcessStatus status = service.getRunningPipelinesProcess(crawlId);

    // Should
    Assert.assertNotNull(status);
    Assert.assertEquals(0, status.getPipelinesSteps().size());
  }

  @Test
  public void testEmptyPipelinesProcessByDatasetId() {
    // State
    String datasetKey = "a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b";

    // When
    Set<PipelinesProcessStatus> set = service.getPipelinesProcessesByDatasetKey(datasetKey);

    // Should
    Assert.assertEquals(0, set.size());
  }

  @Test
  public void testGetRunningPipelinesProcesses() throws Exception {
    // State
    Set<String> crawlIds =
        Sets.newHashSet("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b_1", "be6cd2ff-bcc0-46a5-877e-1fe6e4ef8483_2");
    for (String crawlId : crawlIds) {
      addStatusToZookeeper(crawlId);
    }

    // When
    Set<PipelinesProcessStatus> set = service.getRunningPipelinesProcesses();

    // Should
    Assert.assertEquals(2, set.size());
    set.forEach(status -> {
      Assert.assertNotNull(status);
      Assert.assertEquals(6, status.getPipelinesSteps().size());
      Assert.assertTrue(crawlIds.contains(status.getCrawlId()));
      status.getPipelinesSteps().forEach(step -> {
        Assert.assertTrue(ALL_STEPS.contains(step.getName()));
        Assert.assertNotNull(step.getStartDateTime());
        Assert.assertNotNull(step.getEndDateTime());
        Assert.assertEquals(AVAILABILITY, step.getSuccessful().isAvailability());
        Assert.assertEquals(MESSAGE, step.getSuccessful().getMessage());
      });
    });

    // Postprocess
    for (String crawlId : crawlIds) {
      deleteMonitoringById(crawlId);
    }
  }

  @Test
  public void testPipelinesProcessByCrawlId() throws Exception {
    // State
    String crawlId = "a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b_1";
    addStatusToZookeeper(crawlId);

    // When
    PipelinesProcessStatus status = service.getRunningPipelinesProcess(crawlId);

    // Should
    Assert.assertNotNull(status);
    Assert.assertEquals(6, status.getPipelinesSteps().size());
    Assert.assertEquals(crawlId, status.getCrawlId());
    status.getPipelinesSteps().forEach(step -> {
      Assert.assertTrue(ALL_STEPS.contains(step.getName()));
      Assert.assertNotNull(step.getStartDateTime());
      Assert.assertNotNull(step.getEndDateTime());
      Assert.assertEquals(AVAILABILITY, step.getSuccessful().isAvailability());
      Assert.assertEquals(MESSAGE, step.getSuccessful().getMessage());
    });

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testPipelinesProcessByDatasetId() throws Exception {
    // State
    String datasetId = "a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b";
    String crawlId = "a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b_1";
    addStatusToZookeeper(crawlId);

    // When
    Set<PipelinesProcessStatus> set = service.getPipelinesProcessesByDatasetKey(datasetId);

    // Should
    Assert.assertEquals(1, set.size());
    set.forEach(status -> {
      Assert.assertNotNull(status);
      Assert.assertEquals(6, status.getPipelinesSteps().size());
      Assert.assertEquals(crawlId, status.getCrawlId());
      status.getPipelinesSteps().forEach(step -> {
        Assert.assertTrue(ALL_STEPS.contains(step.getName()));
        Assert.assertNotNull(step.getStartDateTime());
        Assert.assertNotNull(step.getEndDateTime());
        Assert.assertEquals(AVAILABILITY, step.getSuccessful().isAvailability());
        Assert.assertEquals(MESSAGE, step.getSuccessful().getMessage());
      });
    });

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  private void addStatusToZookeeper(String crawlId) throws Exception {
    for (String path : ALL_STEPS) {
      updateMonitoringDate(crawlId, Fn.START_DATE.apply(path));
      updateMonitoringDate(crawlId, Fn.END_DATE.apply(path));
      updateMonitoring(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(path), AVAILABILITY.toString());
      updateMonitoring(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(path), MESSAGE);
    }
  }

  /**
   * Check exists a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private boolean checkExists(String crawlId) throws Exception {
    return curator.checkExists().forPath(crawlId) != null;
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private void deleteMonitoringById(String crawlId) throws Exception {
    String path = getPipelinesInfoPath(crawlId);
    if (checkExists(path)) {
      curator.delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  /**
   * Creates or updates a String value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path child node path
   * @param value some String value
   */
  private void updateMonitoring(String crawlId, String path, String value) throws Exception {
    String fullPath = getPipelinesInfoPath(crawlId, path);
    byte[] bytes = value.getBytes(Charsets.UTF_8);
    if (checkExists(fullPath)) {
      curator.setData().forPath(fullPath, bytes);
    } else {
      curator.create().creatingParentsIfNeeded().forPath(fullPath, bytes);
    }
  }

  /**
   * Creates or updates current LocalDateTime value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path child node path
   */
  private void updateMonitoringDate(String crawlId, String path) throws Exception {
    String value = LocalDateTime.now().atOffset(ZoneOffset.UTC).format(ISO_LOCAL_DATE_TIME);
    updateMonitoring(crawlId, path, value);
  }
}
