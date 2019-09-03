package org.gbif.crawler.ws;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.crawler.pipelines.PipelinesRunningProcessService;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.Set;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;

/**
 * Pipelines monitoring resource HTTP endpoint
 */
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Path("pipelines/process")
public class PipelinesRunningProcessResource {

  private final PipelinesRunningProcessService service;

  @Inject
  public PipelinesRunningProcessResource(PipelinesRunningProcessService service) {
    this.service = service;
  }

  /**
   * Returns information about specific dataset by crawlId
   *
   * @param crawlId is datasetKey_attempt (f10932cc-683e-46ab-93da-9605688a4f27_10)
   */
  @GET
  @Path("crawlId/{crawlId}")
  public PipelineProcess getRunningPipelinesProcess(@PathParam("crawlId") String crawlId) {
    return service.getPipelinesProcess(crawlId);
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
  public void deletePipelinesProcess(@PathParam("crawlId") String crawlId) {
    service.deletePipelinesProcess(crawlId);
  }

  /**
   * Removes pipelines ZK path
   */
  @DELETE
  public void deletePipelinesProcess() {
    service.deleteAllPipelinesProcess();
  }

  /**
   * Returns information about all running datasets
   */
  @GET
  @Path("running")
  public Set<PipelineProcess> getPipelinesProcesses() {
    return service.getPipelinesProcesses();
  }

  /**
   * Returns information about specific dataset by datasetKey
   *
   * @param datasetKey typical dataset UUID
   */
  @GET
  @Path("datasetKey/{datasetKey}")
  public Set<PipelineProcess> getPipelinesProcessesByDatasetKey(@PathParam("datasetKey") String datasetKey) {
    return service.getProcessesByDatasetKey(datasetKey);
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
