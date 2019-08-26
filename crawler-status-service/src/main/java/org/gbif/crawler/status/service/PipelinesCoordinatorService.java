package org.gbif.crawler.status.service;

import org.gbif.crawler.status.service.model.PipelinesStep;

import java.util.Set;
import java.util.UUID;

public interface PipelinesCoordinatorService {

  void runLastAttempt(UUID datasetKey, Set<PipelinesStep.StepName> steps);

  void runPipelineAttempt(UUID datasetKey, Integer attempt, Set<PipelinesStep.StepName> steps);
}
