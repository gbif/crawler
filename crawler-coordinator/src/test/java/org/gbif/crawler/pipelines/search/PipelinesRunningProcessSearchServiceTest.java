package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link PipelinesRunningProcessSearchService}.
 */
public class PipelinesRunningProcessSearchServiceTest {

  private PipelinesRunningProcessSearchService searchService;


  private static PipelineProcess getTestPipelineProcess() {
    UUID datasetKey = UUID.randomUUID();
    String datasetTitle = "Pontaurus";
    int attemptId = 1;
    PipelineProcess pipelineProcess = new PipelineProcess();
    pipelineProcess.setDatasetKey(datasetKey);
    pipelineProcess.setDatasetTitle(datasetTitle);
    pipelineProcess.setAttempt(attemptId);
    PipelineStep step = new PipelineStep();
    step.setType(StepType.HDFS_VIEW);
    step.setState(PipelineStep.Status.RUNNING);
    pipelineProcess.setSteps(Collections.singleton(step));
    return pipelineProcess;
  }

  /**
   * Initializes the cache and test data.
   */
  @Before
  public void init() {
    searchService = new PipelinesRunningProcessSearchService(Files.createTempDir().getPath());
  }

  /**
   * Close the search service.
   */
  @After
  public void tearDown() {
    if(Objects.nonNull(searchService)) {
      searchService.close();
    }
  }

  /**
   * Adds a document a search for it by dataset title.
   */
  @Test
  public void indexAndSearchTest() {
    PipelineProcess pipelineProcess = getTestPipelineProcess();

    searchService.index(pipelineProcess);

    List<String> hits = searchService.searchByDatasetTitle("ponta*", 1, 10);

    Assert.assertEquals(1, hits.size());
    Assert.assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));
  }

  /**
   * Adds a document a search for it by Step status.
   */
  @Test
  public void stepStatusSearchTest() {
    PipelineProcess pipelineProcess = getTestPipelineProcess();

    searchService.index(pipelineProcess);

    List<String> hits =
        searchService.searchByStepStatus(StepType.HDFS_VIEW, PipelineStep.Status.RUNNING, 1, 10);

    Assert.assertEquals(1, hits.size());
    Assert.assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    hits = searchService.searchByStatus(PipelineStep.Status.RUNNING,1, 10);

    Assert.assertEquals(1, hits.size());
    Assert.assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    hits = searchService.searchByStatus(PipelineStep.Status.COMPLETED,1, 10);
    Assert.assertEquals(0, hits.size());

    hits = searchService.searchByStep(StepType.HDFS_VIEW,1, 10);
    Assert.assertEquals(1, hits.size());

    hits = searchService.searchByStep(StepType.INTERPRETED_TO_INDEX,1, 10);
    Assert.assertEquals(0, hits.size());
  }
}
