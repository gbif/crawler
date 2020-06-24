/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.pipelines;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

/**
 * The public interface of the Pipelines monitoring service that provides information about the
 * pipelines processes that are currently running and registered in Zookeeper.
 */
public interface PipelinesRunningProcessService {

  Set<PipelineProcess> getPipelineProcesses();

  PipelineProcess getPipelineProcess(UUID datasetKey);

  void deletePipelineProcess(UUID datasetKey);

  void deleteAllPipelineProcess();

  PipelinesRunningProcessServiceImpl.PipelineProcessSearchResult search(
      @Nullable String datasetTitle,
      @Nullable UUID datasetKey,
      @Nullable List<PipelineStep.Status> stepStatuses,
      @Nullable List<StepType> stepTypes,
      int pageNumber,
      int pageSize);
}
