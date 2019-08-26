package org.gbif.crawler.pipelines;

import java.util.Set;

import org.gbif.crawler.status.service.model.PipelinesProcessStatus;

/**
 * The public interface of the Pipelines monitoring service that provides information about the
 * pipelines processes that are currently running and registered in Zookeeper.
 */
public interface PipelinesRunningProcessService {

  Set<PipelinesProcessStatus> getPipelinesProcesses();

  PipelinesProcessStatus getPipelinesProcess(String crawlId);

  void deletePipelinesProcess(String crawlId);

  void deleteAllPipelinesProcess();

  Set<PipelinesProcessStatus> getProcessesByDatasetKey(String datasetKey);

  // FIXME: should we use only the one in the PipelineCoordinator??
  void restartFailedStepByDatasetKey(String crawlId, String stepName);

  Set<String> getAllStepsNames();
}
