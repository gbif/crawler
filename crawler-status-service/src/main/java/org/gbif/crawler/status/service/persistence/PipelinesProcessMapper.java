package org.gbif.crawler.status.service.persistence;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep;

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.ibatis.annotations.Param;

/** Mapper for {@link PipelinesProcessStatus} entities. */
public interface PipelinesProcessMapper {

  void create(PipelinesProcessStatus proccess);

  PipelinesProcessStatus get(@Param("datasetKey") UUID datasetKey, @Param("attempt") int attempt);

  void addPipelineStep(
      @Param("pipelinesProcessKey") long pipelinesProcessKey, @Param("step") PipelinesStep step);

  List<PipelinesProcessStatus> list(
      @Nullable @Param("datasetKey") UUID datasetKey,
      @Nullable @Param("attempt") Integer attempt,
      @Nullable @Param("page") Pageable page);
}
