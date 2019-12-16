package org.gbif.crawler.pipelines;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.crawler.constants.PipelinesNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

@RunWith(MockitoJUnitRunner.class)
public class PipelinesRunningProcessServiceImplTest {

  private static final long EXECUTION_ID = 1L;
  private static final String MESSAGE = "{\"executionId\": \"" + EXECUTION_ID + "\"}";

  private static final BiConsumer<Set<PipelineProcess>, Set<String>> ASSERT_FN =
      (s, ids) -> {
        Consumer<PipelineStep> checkFn =
            step -> {
              Assert.assertTrue(Arrays.asList(StepType.values()).contains(step.getType()));
              Assert.assertNotNull(step.getStarted());
              Assert.assertNotNull(step.getFinished());
              Assert.assertEquals(PipelineStep.Status.COMPLETED, step.getState());
              Assert.assertEquals(MESSAGE, step.getMessage());
            };

        s.forEach(
            status -> {
              Assert.assertNotNull(status);
              Assert.assertEquals(6, status.getExecutions().iterator().next().getSteps().size());
              Assert.assertTrue(ids.contains(status.getDatasetKey() + "_" + status.getAttempt()));
              status
                  .getExecutions()
                  .iterator()
                  .next()
                  .getSteps()
                  .forEach(
                      step -> {
                        if (step.getType() == StepType.DWCA_TO_VERBATIM
                            || step.getType() == StepType.XML_TO_VERBATIM
                            || step.getType() == StepType.ABCD_TO_VERBATIM
                            || step.getType() == StepType.VERBATIM_TO_INTERPRETED) {
                          checkFn.accept(step);
                        }
                        if (step.getType() == StepType.HDFS_VIEW) {
                          Assert.assertTrue(
                              Arrays.asList(StepType.values()).contains(step.getType()));
                          Assert.assertNotNull(step.getStarted());
                          Assert.assertNull(step.getFinished());
                          Assert.assertEquals(PipelineStep.Status.FAILED, step.getState());
                          Assert.assertEquals(MESSAGE, step.getMessage());
                        }
                        if (step.getType() == StepType.INTERPRETED_TO_INDEX) {
                          Assert.assertTrue(
                              Arrays.asList(StepType.values()).contains(step.getType()));
                          Assert.assertNotNull(step.getStarted());
                          Assert.assertNull(step.getFinished());
                          Assert.assertEquals(PipelineStep.Status.RUNNING, step.getState());
                        }
                      });
            });
      };

  private CuratorFramework curator;
  private TestingServer server;
  private PipelinesRunningProcessServiceImpl service;

  @Before
  public void setup() throws Exception {
    server = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .namespace("crawler")
            .retryPolicy(new RetryOneTime(1))
            .build();
    curator.start();
    service = new PipelinesRunningProcessServiceImpl(curator, Mockito.mock(DatasetService.class));
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    // we wait for the ZK TreeCache to finish since it's executed async and needs curator to be open
    TimeUnit.MILLISECONDS.sleep(350);
    curator.close();
    server.stop();
  }

  @Test
  public void testEmptyGetRunningPipelinesProcesses() {
    // When
    Set<PipelineProcess> set = service.getPipelineProcesses();

    // Should
    Assert.assertEquals(0, set.size());
  }

  @Test
  public void testEmptyPipelinesProcessByCrawlId() {
    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;

    // When
    PipelineProcess status = service.getPipelineProcess(datasetKey, attempt);

    // Should
    Assert.assertNull(status);
  }

  @Test
  public void testEmptyPipelinesProcessByDatasetId() {
    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;

    // When
    Set<PipelineProcess> set = service.getPipelineProcesses(datasetKey);

    // Should
    Assert.assertEquals(0, set.size());
  }

  @Test
  public void testGetRunningPipelinesProcesses() throws Exception {
    // State
    Set<String> crawlIds =
        Sets.newHashSet(
            "a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b_1", "be6cd2ff-bcc0-46a5-877e-1fe6e4ef8483_2");
    for (String crawlId : crawlIds) {
      addStatusToZookeeper(crawlId);
    }

    // we wait for the ZK TreeCache to respond to the events
    TimeUnit.MILLISECONDS.sleep(2000);

    // When
    Set<PipelineProcess> set = service.getPipelineProcesses();

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
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey.toString() + "_" + attempt;
    addStatusToZookeeper(crawlId);

    // we wait for the ZK TreeCache to respond to the events
    TimeUnit.MILLISECONDS.sleep(300);

    // When
    PipelineProcess status = service.getPipelineProcess(datasetKey, attempt);

    // Should
    Assert.assertNotNull(status);
    ASSERT_FN.accept(Collections.singleton(status), Collections.singleton(crawlId));

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testPipelinesProcessByDatasetId() throws Exception {
    // State
    UUID datasetId = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    String crawlId = "a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b_1";
    addStatusToZookeeper(crawlId);

    // we wait for the ZK TreeCache to respond to the events
    TimeUnit.MILLISECONDS.sleep(200);

    // When
    Set<PipelineProcess> set = service.getPipelineProcesses(datasetId);

    // Should
    ASSERT_FN.accept(set, Collections.singleton(crawlId));

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  private void addStatusToZookeeper(String crawlId) throws Exception {
    Consumer<StepType> successfulFn =
        type -> {
          try {
            updateMonitoringDate(crawlId, Fn.START_DATE.apply(type.getLabel()));
            updateMonitoringDate(crawlId, Fn.END_DATE.apply(type.getLabel()));
            updateMonitoring(
                crawlId,
                Fn.SUCCESSFUL_AVAILABILITY.apply(type.getLabel()),
                Boolean.TRUE.toString());
            updateMonitoring(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(type.getLabel()), MESSAGE);
            updateMonitoring(crawlId, Fn.MQ_MESSAGE.apply(type.getLabel()), MESSAGE);
          } catch (Exception ex) {
            throw new RuntimeException(ex.getCause());
          }
        };

    successfulFn.accept(StepType.DWCA_TO_VERBATIM);
    successfulFn.accept(StepType.XML_TO_VERBATIM);
    successfulFn.accept(StepType.ABCD_TO_VERBATIM);
    successfulFn.accept(StepType.VERBATIM_TO_INTERPRETED);

    updateMonitoringDate(crawlId, Fn.START_DATE.apply(StepType.HDFS_VIEW.getLabel()));
    updateMonitoring(
        crawlId,
        Fn.ERROR_AVAILABILITY.apply(StepType.HDFS_VIEW.getLabel()),
        Boolean.TRUE.toString());
    updateMonitoring(crawlId, Fn.ERROR_MESSAGE.apply(StepType.HDFS_VIEW.getLabel()), MESSAGE);
    updateMonitoring(crawlId, Fn.MQ_MESSAGE.apply(StepType.HDFS_VIEW.getLabel()), MESSAGE);

    updateMonitoringDate(crawlId, Fn.START_DATE.apply(StepType.INTERPRETED_TO_INDEX.getLabel()));
    updateMonitoring(crawlId, Fn.MQ_MESSAGE.apply(StepType.INTERPRETED_TO_INDEX.getLabel()), MESSAGE);
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
    String path = PipelinesNodePaths.getPipelinesInfoPath(crawlId);
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
    String fullPath = PipelinesNodePaths.getPipelinesInfoPath(crawlId, path);
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
