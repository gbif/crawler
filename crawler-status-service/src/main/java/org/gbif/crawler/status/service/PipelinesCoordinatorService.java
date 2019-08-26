package org.gbif.crawler.status.service;

import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;

import java.util.Set;
import java.util.UUID;

public interface PipelinesCoordinatorService {

  void runLastAttempt(UUID datasetKey, Set<PipelinesProcessStatus.PipelinesStep.StepName> steps);

  void runPipelineAttempt(UUID datasetKey, Integer attempt, Set<PipelinesProcessStatus.PipelinesStep.StepName> steps);
}
