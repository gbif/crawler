package org.gbif.crawler.pipelines;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.crawler.pipelines.search.PipelinesRunningProcessSearchService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * The public interface of the Pipelines monitoring service that provides information about the
 * pipelines processes that are currently running and registered in Zookeeper.
 */
public interface PipelinesRunningProcessService {

  Set<PipelineProcess> getPipelineProcesses();

  Set<PipelineProcess> getPipelineProcesses(UUID datasetKey);

  PipelineProcess getPipelineProcess(UUID datasetKey, int attempt);

  void deletePipelineProcess(UUID datasetKey, int attempt);

  void deleteAllPipelineProcess();

  PipelinesRunningProcessSearchService.PipelineProcessSearchResult searchByDatasetTitle(String datasetTitleQ, int pageNumber, int pageSize);

}
