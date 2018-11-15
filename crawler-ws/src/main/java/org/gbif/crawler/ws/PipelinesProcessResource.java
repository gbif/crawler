package org.gbif.crawler.ws;

import org.gbif.crawler.pipelines.PipelinesProcessService;
import org.gbif.crawler.pipelines.PipelinesProcessStatus;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;

/**
 * Pipelines monitoring resource HTTP endpoint
 */
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Path("pipelines/process")
public class PipelinesProcessResource {

  private final PipelinesProcessService service;

  @Inject
  public PipelinesProcessResource(PipelinesProcessService service) {
    this.service = service;
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

}
