package org.gbif.crawler.pipelines;

import java.util.Set;

import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;

/**
 * The public interface of the Pipelines monitoring service
 */
public interface PipelinesProcessService {

  Set<PipelinesProcessStatus> getRunningPipelinesProcesses();

  PipelinesProcessStatus getRunningPipelinesProcess(String crawlId);

  void deleteRunningPipelinesProcess(String crawlId);

  void deleteAllRunningPipelinesProcess();

  Set<PipelinesProcessStatus> getPipelinesProcessesByDatasetKey(String datasetKey);

  void restartFailedStepByDatasetKey(String crawlId, String stepName);

  Set<String> getAllStepsNames();

}
