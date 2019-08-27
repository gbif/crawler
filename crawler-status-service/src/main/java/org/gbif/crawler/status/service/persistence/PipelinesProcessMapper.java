package org.gbif.crawler.status.service.persistence;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.crawler.status.service.model.PipelinesProcessStatus;
import org.gbif.crawler.status.service.model.PipelinesStep;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.ibatis.annotations.Param;

/** Mapper for {@link PipelinesProcessStatus} entities. */
public interface PipelinesProcessMapper {

  /**
   * Inserts a new {@link PipelinesProcessStatus}.
   *
   * <p>The id generated is set to the {@link PipelinesProcessStatus} received as parameter.
   *
   * @param proccess to insert
   */
  void create(PipelinesProcessStatus proccess);

  /**
   * Retrieves a {@link PipelinesProcessStatus} by dataset key and attempt.
   *
   * @param datasetKey
   * @param attempt
   * @return {@link PipelinesProcessStatus}
   */
  PipelinesProcessStatus get(@Param("datasetKey") UUID datasetKey, @Param("attempt") int attempt);

  Optional<Integer> getLastAttempt(@Param("datasetKey") UUID datasetKey);

  /**
   * Adds a {@link PipelinesStep} to an existing {@link PipelinesProcessStatus}.
   *
   * @param pipelinesProcessKey key of the process where we want to add the step
   * @param step step to add
   */
  void addPipelineStep(
      @Param("pipelinesProcessKey") long pipelinesProcessKey, @Param("step") PipelinesStep step);

  /**
   * Lists {@link PipelinesProcessStatus} based in the search parameters.
   *
   * <p>It supports paging.
   *
   * @param datasetKey dataset key
   * @param attempt attempt
   * @param page page to specify the offset and the limit
   * @return list of {@link PipelinesProcessStatus}
   */
  List<PipelinesProcessStatus> list(
      @Nullable @Param("datasetKey") UUID datasetKey,
      @Nullable @Param("attempt") Integer attempt,
      @Nullable @Param("page") Pageable page);

  /** Counts the number of {@link PipelinesProcessStatus} based in the search parameters. */
  long count(
      @Nullable @Param("datasetKey") UUID datasetKey, @Nullable @Param("attempt") Integer attempt);

  PipelinesStep getPipelineStep(@Param("key") long key);

  void updatePipelineStepState(
      @Param("stepId") long stepId, @Param("state") PipelinesStep.Status state);
}
