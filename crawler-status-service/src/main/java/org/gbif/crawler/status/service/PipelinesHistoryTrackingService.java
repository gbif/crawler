package org.gbif.crawler.status.service;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.crawler.status.service.model.PipelineStep;
import org.gbif.crawler.status.service.model.PipelineProcess;
import org.gbif.crawler.status.service.model.StepType;

import java.util.Set;
import java.util.UUID;

/**
 * Service to provide the history and re-execute previous attempts of Pipelines.
 */
public interface PipelinesHistoryTrackingService {

  /**
   * Executes the last crawl/pipeline attempt executed on a dataset.
   * @param datasetKey dataset identifier
   * @param steps steps to be executed
   * @param reason textual justification of why it has to be re-executed
   * @return a response containing the request result
   */
  RunPipelineResponse runLastAttempt(UUID datasetKey, Set<StepType> steps, String reason);

  /**
   * Executes a previously run attempt.
   * @param datasetKey dataset identifier
   * @param attempt crawl attempt identifier
   * @param steps steps to be executed
   * @param reason textual justification of why it has to be re-executed
   * @return the response of the execution request
   */
  RunPipelineResponse runPipelineAttempt(UUID datasetKey, Integer attempt, Set<StepType> steps, String reason);

  /**
   * Lists the history of all {@link PipelineProcess}, sorted descending from the most recent one.
   * @param request paging request
   * @return a paged response that contains a list of {@link PipelineProcess}
   */
  PagingResponse<PipelineProcess> history(PagingRequest request);


  /**
   * Lists the history of all {@link PipelineProcess} of a dataset, sorted descending from the most recent one.
   * @param datasetKey dataset identifier
   * @param request paging request
   * @return a paged response that contains a list of {@link PipelineProcess}
   */

  PagingResponse<PipelineProcess> history(UUID datasetKey, PagingRequest request);

  /**
   * Gets the PipelineProcess identified by the the dataset and attempt identifiers.
   * @param datasetKey dataset identifier
   * @param attempt crawl attempt identifier
   * @return a instance of pipelines process if exists
   */
  PipelineProcess get(UUID datasetKey, Integer attempt);

  /**
   * Creates/persists a pipelines process of dataset for an attempt identifier.
   * @param datasetKey dataset identifier
   * @param attempt attempt identifier
   * @param creator user or process that created the pipeline
   * @return a new instance of a pipelines process
   */
  PipelineProcess create(UUID datasetKey, Integer attempt, String creator);

  /**
   * Adds/persists the information of a pipeline step.
   * @param pipelineProcessKey sequential identifier of a pipeline process
   * @param pipelineStep step to be added
   * @return the PipelineStep created
   */
  PipelineStep addPipelineStep(Long pipelineProcessKey, PipelineStep pipelineStep);

  /**
   * Updates the status of a pipeline step.
   * @param pipelineStepKey sequential identifier of a pipeline process step
   * @param status new status for the pipeline step
   */
  void updatePipelineStep(Long pipelineStepKey, PipelineStep.Status status);

}
