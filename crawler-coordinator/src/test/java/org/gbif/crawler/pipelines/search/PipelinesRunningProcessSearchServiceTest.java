package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test class for {@link PipelinesRunningProcessSearchService}. */
public class PipelinesRunningProcessSearchServiceTest {

  private PipelinesRunningProcessSearchService searchService;

  private static PipelineProcess getTestPipelineProcess() {
    UUID datasetKey = UUID.randomUUID();
    String datasetTitle = "Pontaurus one";
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

  /** Initializes the cache and test data. */
  @Before
  public void init() {
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    searchService = new PipelinesRunningProcessSearchService(tmpDir.getPath());
  }

  /** Close the search service. */
  @After
  public void tearDown() {
    if (Objects.nonNull(searchService)) {
      searchService.close();
    }
  }

  /** Adds a document a search for it by dataset title. */
  @Test
  public void indexAndSearchTest() {
    // State
    PipelineProcess pipelineProcess = getTestPipelineProcess();
    searchService.index(pipelineProcess);

    // When
    List<String> hits =
        searchService.search(SearchParams.newBuilder().setDatasetTitle("ponta").build(), 0, 10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(SearchParams.newBuilder().setDatasetTitle("onta on").build(), 0, 10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));
  }

  /** Adds a document a search for it by Step status. */
  @Test
  public void multipleTermsSearchTest() {
    // State
    PipelineProcess pipelineProcess = getTestPipelineProcess();
    searchService.index(pipelineProcess);

    // When
    List<String> hits =
        searchService.search(SearchParams.newBuilder().setDatasetTitle("ponta").build(), 0, 10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .setDatasetTitle("ponta")
                .setDatasetKey(pipelineProcess.getDatasetKey())
                .build(),
            0,
            10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder().setDatasetKey(pipelineProcess.getDatasetKey()).build(),
            0,
            10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .setDatasetTitle("foo")
                .setDatasetKey(pipelineProcess.getDatasetKey())
                .build(),
            0,
            10);

    // Expect
    assertEquals(0, hits.size());

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .setDatasetKey(pipelineProcess.getDatasetKey())
                .addStepType(StepType.HDFS_VIEW)
                .addStatus(PipelineStep.Status.RUNNING)
                .build(),
            0,
            10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .setDatasetKey(pipelineProcess.getDatasetKey())
                .addStepType(StepType.HDFS_VIEW)
                .addStatus(PipelineStep.Status.FAILED)
                .build(),
            0,
            10);

    // Expect
    assertEquals(0, hits.size());

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder().addStepType(StepType.HDFS_VIEW).build(), 0, 10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));
  }

  @Test
  public void updateTest() {
    // State
    PipelineProcess pipelineProcess = getTestPipelineProcess();
    searchService.index(pipelineProcess);

    // When
    PipelineStep step = new PipelineStep();
    step.setStarted(LocalDateTime.now());
    step.setType(StepType.INTERPRETED_TO_INDEX);
    step.setState(PipelineStep.Status.COMPLETED);
    pipelineProcess.getSteps().add(step);
    searchService.update(pipelineProcess);

    List<String> hits =
        searchService.search(
            SearchParams.newBuilder()
                .setStepTypes(Collections.singletonList(StepType.INTERPRETED_TO_INDEX))
                .build(),
            0,
            10);

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));
  }

  @Test
  public void deleteTest() {
    // State
    PipelineProcess pipelineProcess = getTestPipelineProcess();
    searchService.index(pipelineProcess);

    // we add another step to the process to simulate a crawl and check that everything is deleted
    PipelineStep step = new PipelineStep();
    step.setStarted(LocalDateTime.now());
    step.setType(StepType.INTERPRETED_TO_INDEX);
    step.setState(PipelineStep.Status.COMPLETED);
    pipelineProcess.getSteps().add(step);
    searchService.update(pipelineProcess);

    // When
    searchService.delete(pipelineProcess.getDatasetKey() + "_" + pipelineProcess.getAttempt());

    List<String> hits =
        searchService.search(
            SearchParams.newBuilder().setDatasetKey(pipelineProcess.getDatasetKey()).build(),
            0,
            10);

    // Expect
    assertEquals(0, hits.size());
  }
}
