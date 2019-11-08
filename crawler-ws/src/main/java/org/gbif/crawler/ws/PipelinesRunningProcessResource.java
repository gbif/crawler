package org.gbif.crawler.ws;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.crawler.pipelines.PipelinesRunningProcessService;
import org.gbif.crawler.pipelines.search.PipelinesRunningProcessSearchService;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;

/**
 * Pipelines monitoring resource HTTP endpoint
 */
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Path("pipelines/process/running")
public class PipelinesRunningProcessResource {

  private static final int MAX_PAGE_SIZE = 100;

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

  /**
   * Returns information about specific dataset by datasetKey
   *
   * @param datasetKey typical dataset UUID
   */
  @GET
  @Path("search")
  public PipelinesRunningProcessSearchService.PipelineProcessSearchResult searchByDatasetTitle(@QueryParam("datasetTile") String datasetTitleQ, @QueryParam("page") int pageNumber, @QueryParam("size") int pageSize) {
    return service.searchByDatasetTitle(datasetTitleQ, pageNumber, Math.min(pageSize, MAX_PAGE_SIZE));
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
