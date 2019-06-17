package org.gbif.crawler.pipelines;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.pipelines.PipelinesProcessStatus.PipelinesStep;
import org.gbif.crawler.pipelines.PipelinesProcessStatus.PipelinesStep.Status;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

import static org.gbif.crawler.constants.PipelinesNodePaths.ABCD_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.ALL_STEPS;
import static org.gbif.crawler.constants.PipelinesNodePaths.DWCA_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.HIVE_VIEW;
import static org.gbif.crawler.constants.PipelinesNodePaths.INTERPRETED_TO_INDEX;
import static org.gbif.crawler.constants.PipelinesNodePaths.VERBATIM_TO_INTERPRETED;
import static org.gbif.crawler.constants.PipelinesNodePaths.XML_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

@RunWith(MockitoJUnitRunner.class)
public class PipelinesProcessServiceImplTest {

  private static final String MESSAGE = "info";

  private static final BiConsumer<Set<PipelinesProcessStatus>, Set<String>> ASSERT_FN = (s, ids) -> {
    Consumer<PipelinesStep> checkFn = step -> {
      Assert.assertTrue(ALL_STEPS.contains(step.getName()));
      Assert.assertNotNull(step.getStarted());
      Assert.assertNotNull(step.getFinished());
      Assert.assertEquals(Status.COMPLETED, step.getState());
      Assert.assertEquals(MESSAGE, step.getMessage());
    };

    s.forEach(status -> {
      Assert.assertNotNull(status);
      Assert.assertEquals(6, status.getPipelinesSteps().size());
      Assert.assertTrue(ids.contains(status.getCrawlId()));
      status.getPipelinesSteps().forEach(step -> {

        if (step.getName().equals(DWCA_TO_VERBATIM)
            || step.getName().equals(XML_TO_VERBATIM)
            || step.getName().equals(ABCD_TO_VERBATIM)
            || step.getName().equals(VERBATIM_TO_INTERPRETED)) {
          checkFn.accept(step);
        }
        if (step.getName().equals(HIVE_VIEW)) {
          Assert.assertTrue(ALL_STEPS.contains(step.getName()));
          Assert.assertNotNull(step.getStarted());
          Assert.assertNull(step.getFinished());
          Assert.assertEquals(Status.FAILED, step.getState());
          Assert.assertEquals(MESSAGE, step.getMessage());
        }
        if (step.getName().equals(INTERPRETED_TO_INDEX)) {
          Assert.assertTrue(ALL_STEPS.contains(step.getName()));
          Assert.assertNotNull(step.getStarted());
          Assert.assertNull(step.getFinished());
          Assert.assertEquals(Status.RUNNING, step.getState());
          Assert.assertNull(MESSAGE, step.getMessage());
        }
      });
    });
  };


  private CuratorFramework curator;
  private TestingServer server;
  private PipelinesProcessServiceImpl service;


  @Before
  public void setup() throws Exception {
    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder()
        .connectString(server.getConnectString())
        .namespace("crawler")
        .retryPolicy(new RetryOneTime(1))
        .build();
    curator.start();
    service =
        new PipelinesProcessServiceImpl(curator, Executors.newSingleThreadExecutor(), null, "test");
  }

  @After
  public void tearDown() throws IOException {
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
    ASSERT_FN.accept(set, crawlIds);

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
    ASSERT_FN.accept(Collections.singleton(status), Collections.singleton(crawlId));

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
    ASSERT_FN.accept(set, Collections.singleton(crawlId));

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  private void addStatusToZookeeper(String crawlId) throws Exception {
    Consumer<String> successfulFn = path -> {
      try {
        updateMonitoringDate(crawlId, Fn.START_DATE.apply(path));
        updateMonitoringDate(crawlId, Fn.END_DATE.apply(path));
        updateMonitoring(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(path), Boolean.TRUE.toString());
        updateMonitoring(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(path), MESSAGE);
      } catch (Exception ex) {
        throw new RuntimeException(ex.getCause());
      }
    };

    successfulFn.accept(DWCA_TO_VERBATIM);
    successfulFn.accept(XML_TO_VERBATIM);
    successfulFn.accept(ABCD_TO_VERBATIM);
    successfulFn.accept(VERBATIM_TO_INTERPRETED);

    updateMonitoringDate(crawlId, Fn.START_DATE.apply(HIVE_VIEW));
    updateMonitoring(crawlId, Fn.ERROR_AVAILABILITY.apply(HIVE_VIEW), Boolean.TRUE.toString());
    updateMonitoring(crawlId, Fn.ERROR_MESSAGE.apply(HIVE_VIEW), MESSAGE);

    updateMonitoringDate(crawlId, Fn.START_DATE.apply(INTERPRETED_TO_INDEX));
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
