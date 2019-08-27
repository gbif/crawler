package org.gbif.crawler.ws;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.crawler.status.service.PipelinesHistoryTrackingService;
import org.gbif.crawler.status.service.RunPipelineResponse;
import org.gbif.crawler.status.service.model.PipelineProcess;
import org.gbif.crawler.status.service.model.PipelineStep;
import org.gbif.crawler.status.service.model.StepType;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.inject.Inject;

/**
 * Pipelines History service.
 */
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Path("pipelines/history")
public class PipelinesHistoryResource {

  private final PipelinesHistoryTrackingService historyTrackingService;

  @Inject
  public PipelinesHistoryResource(PipelinesHistoryTrackingService historyTrackingService) {
    this.historyTrackingService = historyTrackingService;
  }

  /**
   * Transforms a {@link RunPipelineResponse} into a {@link Response}.
   */
  private static Response toHttpResponse(RunPipelineResponse runPipelineResponse) {
    if (runPipelineResponse.getResponseStatus() == RunPipelineResponse.ResponseStatus.PIPELINE_IN_SUBMITTED) {
      return Response.status(Response.Status.BAD_REQUEST).entity(runPipelineResponse).build();
    } else if (runPipelineResponse.getResponseStatus() == RunPipelineResponse.ResponseStatus.UNSUPPORTED_STEP) {
      return Response.status(Response.Status.NOT_ACCEPTABLE).entity(runPipelineResponse).build();
    } else if (runPipelineResponse.getResponseStatus() == RunPipelineResponse.ResponseStatus.ERROR) {
      return Response.serverError().entity(runPipelineResponse).build();
    }

    return Response.ok().entity(runPipelineResponse).build();

  }

  /**
   * Lists the history of all pipelines.
   */
  @GET
  public PagingResponse<PipelineProcess> history(@Context PagingRequest pagingRequest) {
    return historyTrackingService.history(pagingRequest);
  }

  /**
   * Lists teh history of a dataset.
   */
  @GET
  @Path("{datasetKey}")
  public PagingResponse<PipelineProcess> history(@PathParam("datasetKey") String datasetKey, @Context PagingRequest pagingRequest) {
    return historyTrackingService.history(UUID.fromString(datasetKey), pagingRequest);
  }

  /**
   * Gets the data of a {@link PipelineProcess}.
   */
  @GET
  @Path("{datasetKey}/{attempt}")
  public PipelineProcess get(@PathParam("datasetKey") String datasetKey, @PathParam("attempt") String attempt) {
   return historyTrackingService.get(UUID.fromString(datasetKey), Integer.parseInt(attempt));
  }

  /**
   * Re-run a pipeline step.
   */
  @POST
  @Path("{datasetKey}/{attempt}")
  public Response runPipelineAttempt(@PathParam("datasetKey") String datasetKey, @PathParam("attempt") String attempt,
                                                @QueryParam("steps") String steps, @QueryParam("reason") String reason) {

    return  toHttpResponse(historyTrackingService.runPipelineAttempt(UUID.fromString(datasetKey),
                                                                                        Integer.parseInt(attempt),
                                                                                        Arrays.stream(steps.split(","))
                                                                                          .map(StepType::valueOf)
                                                                                          .collect(Collectors.toSet()), reason));
  }

  /**
   * Adds a new pipeline step.
   */
  @POST
  @Path("process/{processKey}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void addPipelineStep(@PathParam("processKey") String processKey, @Context PipelineStep pipelineStep) {
    historyTrackingService.addPipelineStep(Long.parseLong(processKey), pipelineStep);
  }


  /**
   * Updates the step status.
   */
  @POST
  @Path("process/{processKey}/{status}")
  public void updatePipelineStep(@PathParam("processKey") String processKey, @PathParam("status") String status) {
    historyTrackingService.updatePipelineStep(Long.parseLong(processKey), PipelineStep.Status.valueOf(status.toUpperCase()));
  }


  /**
   * Restart last failed pipelines step
   */
  @POST
  @Path("{datasetKey}")
  public Response runPipelineAttempt(@PathParam("datasetKey") String datasetKey, @QueryParam("steps") String steps,
                                 @QueryParam("reason") String reason) {
    return toHttpResponse(historyTrackingService.runLastAttempt(UUID.fromString(datasetKey),
                                                                Arrays.stream(steps.split(","))
                                                                  .map(StepType::valueOf)
                                                                  .collect(Collectors.toSet()),
                                                                reason));
  }

}
