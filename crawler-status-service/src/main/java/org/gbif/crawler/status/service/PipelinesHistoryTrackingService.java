package org.gbif.crawler.status.service;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.crawler.status.service.model.PipelineStep;
import org.gbif.crawler.status.service.model.PipelineProcess;
import org.gbif.crawler.status.service.model.StepType;

import java.util.Set;
import java.util.UUID;

public interface PipelinesHistoryTrackingService {

  ReRunPipelineResponse runLastAttempt(UUID datasetKey, Set<StepType> actions, String reason);

  ReRunPipelineResponse runPipelineAttempt(UUID datasetKey, Integer attempt, Set<StepType> steps, String reason);

  PagingResponse<PipelineProcess> history(PagingRequest request);

  PagingResponse<PipelineProcess> history(UUID datasetKey, PagingRequest request);

  PipelineProcess get(UUID datasetKey, Integer attempt);

  PipelineProcess create(UUID datasetKey, int attempt, String creator);

  PipelineStep addPipelineAction(long pipelineProcessKey, PipelineStep pipelineStep);

  void updatePipelineAction(long pipelineStepKey, PipelineStep.Status status);

}
