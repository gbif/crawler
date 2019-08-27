package org.gbif.crawler.status.service.persistence;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.crawler.status.service.model.PipelineProcess;
import org.gbif.crawler.status.service.model.PipelineStep;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.ibatis.annotations.Param;

/** Mapper for {@link PipelineProcess} entities. */
public interface PipelineProcessMapper {

  /**
   * Inserts a new {@link PipelineProcess}.
   *
   * <p>The id generated is set to the {@link PipelineProcess} received as parameter.
   *
   * @param proccess to insert
   */
  void create(PipelineProcess proccess);

  /**
   * Retrieves a {@link PipelineProcess} by dataset key and attempt.
   *
   * @param datasetKey
   * @param attempt
   * @return {@link PipelineProcess}
   */
  PipelineProcess get(@Param("datasetKey") UUID datasetKey, @Param("attempt") int attempt);

  Optional<Integer> getLastAttempt(@Param("datasetKey") UUID datasetKey);

  /**
   * Adds a {@link PipelineStep} to an existing {@link PipelineProcess}.
   *
   * @param pipelinesProcessKey key of the process where we want to add the step
   * @param step step to add
   */
  void addPipelineStep(@Param("pipelinesProcessKey") long pipelinesProcessKey, @Param("step") PipelineStep step);

  /**
   * Lists {@link PipelineProcess} based in the search parameters.
   *
   * <p>It supports paging.
   *
   * @param datasetKey dataset key
   * @param attempt attempt
   * @param page page to specify the offset and the limit
   * @return list of {@link PipelineProcess}
   */
  List<PipelineProcess> list(@Nullable @Param("datasetKey") UUID datasetKey, @Nullable @Param("attempt") Integer attempt,
                             @Nullable @Param("page") Pageable page);

  /** Counts the number of {@link PipelineProcess} based in the search parameters. */
  long count(@Nullable @Param("datasetKey") UUID datasetKey, @Nullable @Param("attempt") Integer attempt);

  PipelineStep getPipelineStep(@Param("key") long key);

  void updatePipelineStepState(@Param("stepId") long stepId, @Param("state") PipelineStep.Status state);
}