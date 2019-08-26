package org.gbif.crawler.ws;

import org.gbif.crawler.pipelines.PipelinesProcessService;
import org.gbif.crawler.status.service.PipelinesCoordinatorService;
import org.gbif.crawler.status.service.model.PipelinesProcessStatus;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;

/**
 * Pipelines monitoring resource HTTP endpoint
 */
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Path("pipelines/process")
public class PipelinesProcessResource {

  private final PipelinesProcessService service;

  private final PipelinesCoordinatorService coordinatorService;

  @Inject
  public PipelinesProcessResource(PipelinesProcessService service, PipelinesCoordinatorService coordinatorService) {
    this.service = service;
    this.coordinatorService = coordinatorService;
  }

  /**
   * Returns information about specific dataset by crawlId
   *
   * @param crawlId is datasetKey_attempt (f10932cc-683e-46ab-93da-9605688a4f27_10)
   */
  @GET
  @Path("crawlId/{crawlId}")
  public PipelinesProcessStatus getRunningPipelinesProcess(@PathParam("crawlId") String crawlId) {
    return service.getRunningPipelinesProcess(crawlId);
  }


  /**
   * Restart last failed pipelines step
   */
  @POST
  @Path("crawlId/{crawlId}/restart/{stepName}")
  public void restartFailedStepByDatasetKey(@PathParam("crawlId") String crawlId, @PathParam("stepName") String stepName) {
    service.restartFailedStepByDatasetKey(crawlId, stepName);
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId is datasetKey_attempt (f10932cc-683e-46ab-93da-9605688a4f27_10)
   */
  @DELETE
  @Path("crawlId/{crawlId}")
  public void deleteRunningPipelinesProcess(@PathParam("crawlId") String crawlId) {
    service.deleteRunningPipelinesProcess(crawlId);
  }

  /**
   * Removes pipelines ZK path
   */
  @DELETE
  public void deleteRunningPipelinesProcess() {
    service.deleteAllRunningPipelinesProcess();
  }

  /**
   * Returns information about all running datasets
   */
  @GET
  @Path("running")
  public Set<PipelinesProcessStatus> getRunningPipelinesProcesses() {
    return service.getRunningPipelinesProcesses();
  }

  /**
   * Returns information about specific dataset by datasetKey
   *
   * @param datasetKey typical dataset UUID
   */
  @GET
  @Path("datasetKey/{datasetKey}")
  public Set<PipelinesProcessStatus> getPipelinesProcessesByDatasetKey(@PathParam("datasetKey") String datasetKey) {
    return service.getPipelinesProcessesByDatasetKey(datasetKey);
  }


  /**
   * Restart last failed pipelines step
   */
  @POST
  @Path("datasetKey/{datasetKey}/{crawlId}")
  public void reRunPipeline(@PathParam("datasetKey") String datasetKey, @PathParam("crawlId") String crawlId, @QueryParam("steps") String steps) {
    coordinatorService.runPipelineAttempt(UUID.fromString(datasetKey), Integer.parseInt(crawlId),
                                          Arrays.stream(steps.split(","))
                                             .map(PipelinesProcessStatus.PipelinesStep.StepName::valueOf)
                                             .collect(Collectors.toSet()));
  }

  /**
   * Returns list of pipelines steps names
   */
  @GET
  @Path("steps")
  public Set<String> getAllStepsNames() {
    return service.getAllStepsNames();
  }

}
