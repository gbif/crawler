package org.gbif.crawler.status.service.persistence;

import java.util.UUID;

import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep;

import org.apache.ibatis.annotations.Param;

/**
 * Mapper for {@link PipelinesProcessStatus} entities.
 */
public interface PipelinesProcessStatusMapper {

  void create(PipelinesProcessStatus proccess);

  PipelinesProcessStatus get(@Param("datasetKey") UUID datasetKey, @Param("attempt") int attempt);

  void addPipelineStep(@Param("datasetKey") UUID datasetKey, @Param("attempt") int attempt,
      @Param("step") PipelinesStep step);

}
