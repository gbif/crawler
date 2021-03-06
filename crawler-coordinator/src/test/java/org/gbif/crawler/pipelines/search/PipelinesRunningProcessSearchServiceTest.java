/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test class for {@link PipelinesRunningProcessSearchService}. */
public class PipelinesRunningProcessSearchServiceTest {

  private PipelinesRunningProcessSearchService searchService;

  private static PipelineProcess getTestPipelineProcess() {
    UUID datasetKey = UUID.randomUUID();
    String datasetTitle = "Pontaurus dataset";
    int attemptId = 1;
    PipelineProcess pipelineProcess = new PipelineProcess();
    pipelineProcess.setDatasetKey(datasetKey);
    pipelineProcess.setDatasetTitle(datasetTitle);
    pipelineProcess.setAttempt(attemptId);

    PipelineExecution execution = new PipelineExecution().setCreated(LocalDateTime.now());
    pipelineProcess.addExecution(execution);

    PipelineStep step = new PipelineStep();
    step.setType(StepType.HDFS_VIEW);
    step.setState(PipelineStep.Status.RUNNING);
    execution.addStep(step);
    return pipelineProcess;
  }

  /** Initializes the cache and test data. */
  @BeforeEach
  public void init() {
    searchService = new PipelinesRunningProcessSearchService();
  }

  /** Close the search service. */
  @AfterEach
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
        searchService.search(SearchParams.newBuilder().setDatasetTitle("ponta").build());

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits = searchService.search(SearchParams.newBuilder().setDatasetTitle("onta dat").build());

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
    PipelineProcess pipelineProcess2 = getTestPipelineProcess();
    pipelineProcess2.setDatasetTitle("another dataset");
    pipelineProcess2
        .getExecutions()
        .iterator()
        .next()
        .getSteps()
        .iterator()
        .next()
        .setType(StepType.VERBATIM_TO_INTERPRETED)
        .setState(PipelineStep.Status.COMPLETED);
    searchService.index(pipelineProcess2);

    // When
    List<String> hits =
        searchService.search(SearchParams.newBuilder().setDatasetTitle("ponta").build());

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .setDatasetTitle("ponta")
                .setDatasetKey(pipelineProcess.getDatasetKey())
                .build());

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder().setDatasetKey(pipelineProcess.getDatasetKey()).build());

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .setDatasetTitle("foo")
                .setDatasetKey(pipelineProcess.getDatasetKey())
                .build());

    // Expect
    assertEquals(0, hits.size());

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .setDatasetKey(pipelineProcess.getDatasetKey())
                .addStepType(StepType.HDFS_VIEW)
                .addStatus(PipelineStep.Status.RUNNING)
                .build());

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
                .build());

    // Expect
    assertEquals(0, hits.size());

    // When
    hits = searchService.search(SearchParams.newBuilder().addStepType(StepType.HDFS_VIEW).build());

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .addStepType(StepType.HDFS_VIEW)
                .addStepType(StepType.VERBATIM_TO_INTERPRETED)
                .build());

    // Expect
    assertEquals(2, hits.size());

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder()
                .addStatus(PipelineStep.Status.RUNNING)
                .addStatus(PipelineStep.Status.COMPLETED)
                .build());

    // Expect
    assertEquals(2, hits.size());

    // When
    hits = searchService.search(SearchParams.newBuilder().setDatasetTitle("dataset").build());

    // Expect
    assertEquals(2, hits.size());
  }

  @Test
  public void duplicatesTest() {
    // State
    PipelineProcess pipelineProcess = getTestPipelineProcess();
    searchService.index(pipelineProcess);
    searchService.index(pipelineProcess);

    // When
    List<String> hits =
        searchService.search(
            SearchParams.newBuilder().setDatasetKey(pipelineProcess.getDatasetKey()).build());
    // Expect
    assertEquals(2, hits.size());

    // When
    searchService.delete(pipelineProcess.getDatasetKey() + "_" + pipelineProcess.getAttempt());

    // Expect
    hits =
        searchService.search(
            SearchParams.newBuilder().setDatasetKey(pipelineProcess.getDatasetKey()).build());
    assertEquals(0, hits.size());
  }

  @Test
  public void updateTest() {
    // State
    PipelineProcess pipelineProcess = getTestPipelineProcess();
    searchService.index(pipelineProcess);

    // When
    pipelineProcess
        .getExecutions()
        .iterator()
        .next()
        .getSteps()
        .iterator()
        .next()
        .setState(PipelineStep.Status.COMPLETED);

    PipelineStep step = new PipelineStep();
    step.setStarted(LocalDateTime.now());
    step.setType(StepType.INTERPRETED_TO_INDEX);
    step.setState(PipelineStep.Status.COMPLETED);
    pipelineProcess.getExecutions().iterator().next().getSteps().add(step);
    searchService.update(pipelineProcess);

    List<String> hits =
        searchService.search(
            SearchParams.newBuilder()
                .setStepTypes(Collections.singletonList(StepType.INTERPRETED_TO_INDEX))
                .build());

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder().addStatus(PipelineStep.Status.COMPLETED).build());

    // Expect
    assertEquals(1, hits.size());
    assertTrue(hits.get(0).startsWith(pipelineProcess.getDatasetKey().toString()));

    // When
    hits =
        searchService.search(
            SearchParams.newBuilder().addStatus(PipelineStep.Status.RUNNING).build());

    // Expect
    assertEquals(0, hits.size());
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
    pipelineProcess.getExecutions().iterator().next().getSteps().add(step);
    searchService.update(pipelineProcess);

    // When
    searchService.delete(pipelineProcess.getDatasetKey() + "_" + pipelineProcess.getAttempt());

    List<String> hits =
        searchService.search(
            SearchParams.newBuilder().setDatasetKey(pipelineProcess.getDatasetKey()).build());

    // Expect
    assertEquals(0, hits.size());
  }
}
