package org.gbif.crawler.ws;

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
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;

/**
 * Pipelines monitoring resource HTTP endpoint
 */
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Path("pipelines/process/running")
public class PipelinesRunningProcessResource {

  private static final int DEFAULT_PAGE_SIZE = 10;

  private final PipelinesRunningProcessService service;

  @Inject
  public PipelinesRunningProcessResource(PipelinesRunningProcessService service) {
    this.service = service;
  }

  /**
   * Returns information about all running datasets
   */
  @GET
  public Set<PipelineProcess> getPipelinesProcesses() {
    return service.getPipelineProcesses();
  }

  /** Searchs for the received parameters. */
  @GET
  @Path("query")
  public PipelinesRunningProcessServiceImpl.PipelineProcessSearchResult search(
      @QueryParam("datasetTitle") String datasetTitle,
      @QueryParam("datasetKey") UUID datasetKey,
      @QueryParam("status") List<PipelineStep.Status> statuses,
      @QueryParam("step") List<StepType> stepTypes,
      @Nullable @QueryParam("offset") Integer offset,
      @Nullable @QueryParam("limit") Integer limit) {
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
  @GET
  @Path("{datasetKey}")
  public Set<PipelineProcess> getPipelinesProcessesByDatasetKey(@PathParam("datasetKey") UUID datasetKey) {
    return service.getPipelineProcesses(datasetKey);
  }

  /**
   * Returns information about specific running process.
   *
   * @param datasetKey dataset of the process
   * @param attempt attempt of the process
   */
  @GET
  @Path("{datasetKey}/{attempt}")
  public PipelineProcess getRunningPipelinesProcess(@PathParam("datasetKey") UUID datasetKey,
                                                    @PathParam("attempt") int attempt) {
    return service.getPipelineProcess(datasetKey, attempt);
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param datasetKey dataset of the process
   * @param attempt attempt of the process
   * */
  @DELETE
  @Path("{datasetKey}/{attempt}")
  public void deletePipelinesProcess(@PathParam("datasetKey") UUID datasetKey,
                                     @PathParam("attempt") int attempt) {
    service.deletePipelineProcess(datasetKey, attempt);
  }

  /**
   * Removes pipelines ZK path
   */
  @DELETE
  public void deletePipelinesProcess() {
    service.deleteAllPipelineProcess();
  }
}
