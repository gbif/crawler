package org.gbif.crawler.pipelines;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * The public interface of the Pipelines monitoring service that provides information about the
 * pipelines processes that are currently running and registered in Zookeeper.
 */
public interface PipelinesRunningProcessService {

  Set<PipelineProcess> getPipelineProcesses();

  Set<PipelineProcess> getPipelineProcesses(UUID datasetKey);

  PipelineProcess getPipelineProcess(UUID datasetKey);

  void deletePipelineProcess(UUID datasetKey);

  void deleteAllPipelineProcess();

  PipelinesRunningProcessServiceImpl.PipelineProcessSearchResult search(
      @Nullable String datasetTitle,
      @Nullable UUID datasetKey,
      @Nullable List<PipelineStep.Status> stepStatuses,
      @Nullable List<StepType> stepTypes,
      int pageNumber,
      int pageSize);
}
