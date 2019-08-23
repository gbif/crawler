package org.gbif.crawler.status.service.persistence;

import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep;

import java.util.UUID;

import org.apache.ibatis.annotations.Param;

/**
 * Mapper for {@link PipelinesProcessStatus} entities.
 */
public interface PipelinesProcessMapper {

  void create(PipelinesProcessStatus proccess);

  PipelinesProcessStatus get(@Param("datasetKey") UUID datasetKey, @Param("attempt") int attempt);

  void addPipelineStep(@Param("pipelinesProcessKey") long pipelinesProcessKey, @Param("step") PipelinesStep step);

  // TODO: list all

}
