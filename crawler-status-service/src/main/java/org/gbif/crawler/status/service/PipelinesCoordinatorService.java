package org.gbif.crawler.status.service;

import org.gbif.crawler.status.service.model.StepName;

import java.util.Set;
import java.util.UUID;

public interface PipelinesCoordinatorService {

  void runLastAttempt(UUID datasetKey, Set<StepName> steps);

  void runPipelineAttempt(UUID datasetKey, Integer attempt, Set<StepName> steps);
}
