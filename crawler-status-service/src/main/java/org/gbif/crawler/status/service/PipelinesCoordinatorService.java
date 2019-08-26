package org.gbif.crawler.status.service;

import org.gbif.crawler.status.service.model.StepName;

import java.util.Set;
import java.util.UUID;

public interface PipelinesCoordinatorService {

  ReRunPipelineResponse runLastAttempt(UUID datasetKey, Set<StepName> steps);

  ReRunPipelineResponse runPipelineAttempt(UUID datasetKey, Integer attempt, Set<StepName> steps);
}
