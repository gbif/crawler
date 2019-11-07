package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.google.common.io.Files;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipelinesRunningProcessSearchServiceTest {

  private PipelinesRunningProcessSearchService searchService;

  private Cache<String,PipelineProcess> dataCache;


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
    dataCache.put("1", pipelineProcess);
    searchService = new PipelinesRunningProcessSearchService(Files.createTempDir().getPath());
  }
  
  @After
  public void tearDown() {
    if(Objects.nonNull(searchService)) {
      searchService.close();
    }
  }

  @Test
  public void indexAndSearchTest() {
    PipelineProcess pipelineProcess = dataCache.get("1");

    searchService.index(pipelineProcess);

    List<PipelineProcess>  pipelineProcesses = searchService.searchByDatasetTitle("ponta*", 1, 10, dataCache);

    Assert.assertEquals(1, pipelineProcesses.size());
    Assert.assertEquals(pipelineProcess.getDatasetTitle(), pipelineProcesses.get(0).getDatasetTitle());
  }
}
