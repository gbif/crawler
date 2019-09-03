package org.gbif.crawler.pipelines;

import org.gbif.api.model.pipelines.PipelineProcess;

import java.util.Set;

/**
 * The public interface of the Pipelines monitoring service that provides information about the
 * pipelines processes that are currently running and registered in Zookeeper.
 */
public interface PipelinesRunningProcessService {

  Set<PipelineProcess> getPipelinesProcesses();

  PipelineProcess getPipelinesProcess(String crawlId);

  void deletePipelinesProcess(String crawlId);

  void deleteAllPipelinesProcess();

  Set<PipelineProcess> getProcessesByDatasetKey(String datasetKey);

  // FIXME: should we use only the one in the PipelineCoordinator??
  void restartFailedStepByDatasetKey(String crawlId, String stepName);

  Set<String> getAllStepsNames();
}
