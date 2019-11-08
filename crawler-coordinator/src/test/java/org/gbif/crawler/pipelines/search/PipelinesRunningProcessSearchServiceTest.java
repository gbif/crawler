package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.util.Collections;
import java.util.Objects;
import java.util.UUID;

import com.google.common.io.Files;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link PipelinesRunningProcessSearchService}.
 */
public class PipelinesRunningProcessSearchServiceTest {

  private PipelinesRunningProcessSearchService searchService;

  private Cache<String,PipelineProcess> dataCache;

  /**
   * Initializes the cache and test data.
   */
  @Before
  public void init() {
    dataCache = Cache2kBuilder.of(String.class, PipelineProcess.class)
                .entryCapacity(2)
                .suppressExceptions(false)
                .eternal(true)
                .build();
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
    dataCache.put("1", pipelineProcess);
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
    PipelineProcess pipelineProcess = dataCache.get("1");

    searchService.index(pipelineProcess);

    PipelinesRunningProcessSearchService.PipelineProcessSearchResult  searchResult = searchService.searchByDatasetTitle("ponta*", 1, 10, dataCache);

    Assert.assertEquals(1, searchResult.getTotalHits());
    Assert.assertEquals(pipelineProcess.getDatasetTitle(), searchResult.getResults().get(0).getDatasetTitle());
  }

  /**
   * Adds a document a search for it by Step status.
   */
  @Test
  public void stepStatusSearchTest() {
    PipelineProcess pipelineProcess = dataCache.get("1");

    searchService.index(pipelineProcess);

    PipelinesRunningProcessSearchService.PipelineProcessSearchResult searchResult = searchService.searchByStepStatus(StepType.HDFS_VIEW,
                                                                                                                      PipelineStep.Status.RUNNING,
                                                                                                                      1, 10, dataCache);

    Assert.assertEquals(1, searchResult.getTotalHits());
    Assert.assertEquals(pipelineProcess.getDatasetTitle(), searchResult.getResults().get(0).getDatasetTitle());


    searchResult = searchService.searchByStatus(PipelineStep.Status.RUNNING,1, 10, dataCache);


    Assert.assertEquals(1, searchResult.getTotalHits());
    Assert.assertEquals(pipelineProcess.getDatasetTitle(), searchResult.getResults().get(0).getDatasetTitle());


    searchResult = searchService.searchByStatus(PipelineStep.Status.COMPLETED,1, 10, dataCache);


    Assert.assertEquals(0, searchResult.getTotalHits());
  }
}
