package org.gbif.crawler.ws.resources;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.crawler.pipelines.PipelinesRunningProcessService;
import org.gbif.crawler.pipelines.PipelinesRunningProcessServiceImpl;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Pipelines monitoring resource HTTP endpoint
 */
@Primary
@RestController
@RequestMapping(value = "pipelines/process/running", produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"})
public class PipelinesRunningProcessResource {

  private static final int DEFAULT_PAGE_SIZE = 10;

  private final PipelinesRunningProcessService service;

  @Autowired
  public PipelinesRunningProcessResource(PipelinesRunningProcessService service) {
    this.service = service;
  }

  /**
   * Returns information about all running datasets
   */
  @GetMapping
  public Set<PipelineProcess> getPipelinesProcesses() {
    return service.getPipelineProcesses();
  }

  /** Searchs for the received parameters. */
  @GetMapping("query")
  public PipelinesRunningProcessServiceImpl.PipelineProcessSearchResult search(
      @RequestParam("datasetTitle") String datasetTitle,
      @RequestParam("datasetKey") UUID datasetKey,
      @RequestParam("status") List<PipelineStep.Status> statuses,
      @RequestParam("step") List<StepType> stepTypes,
      @Nullable @RequestParam(value = "offset", required = false) Integer offset,
      @Nullable @RequestParam(value = "limit", required = false) Integer limit) {
    return service.search(
        datasetTitle,
        datasetKey,
        statuses,
        stepTypes,
        offset != null ? offset : 0,
        limit != null ? limit : DEFAULT_PAGE_SIZE);
  }

  /**
   * Returns information about specific dataset by datasetKey
   *
   * @param datasetKey typical dataset UUID
   */
  @GetMapping("{datasetKey}")
  public Set<PipelineProcess> getPipelinesProcessesByDatasetKey(@PathVariable UUID datasetKey) {
    return service.getPipelineProcesses(datasetKey);
  }

  /**
   * Returns information about specific running process.
   *
   * @param datasetKey dataset of the process
   * @param attempt attempt of the process
   */
  @GetMapping("{datasetKey}/{attempt}")
  public PipelineProcess getRunningPipelinesProcess(@PathVariable("datasetKey") UUID datasetKey,
                                                    @PathVariable("attempt") int attempt) {
    return service.getPipelineProcess(datasetKey, attempt);
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param datasetKey dataset of the process
   * @param attempt attempt of the process
   * */
  @DeleteMapping("{datasetKey}/{attempt}")
  public void deletePipelinesProcess(@PathVariable("datasetKey") UUID datasetKey,
                                     @PathVariable("attempt") int attempt) {
    service.deletePipelineProcess(datasetKey, attempt);
  }

  /**
   * Removes pipelines ZK path
   */
  @DeleteMapping
  public void deletePipelinesProcess() {
    service.deleteAllPipelineProcess();
  }
}
