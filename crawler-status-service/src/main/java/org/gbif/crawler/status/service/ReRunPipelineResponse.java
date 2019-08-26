package org.gbif.crawler.status.service;

import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;

import java.util.Set;

public class ReRunPipelineResponse {

  public enum ResponseStatus {
    OK,
    PIPELINE_IN_SUBMITTED,
    UNSUPPORTED_STEP,
    ERROR
  }


  private final ResponseStatus responseStatus;

  private final Set<PipelinesProcessStatus.PipelinesStep.StepName> steps;

  public ReRunPipelineResponse(ResponseStatus responseStatus, Set<PipelinesProcessStatus.PipelinesStep.StepName> steps) {
    this.responseStatus = responseStatus;
    this.steps = steps;
  }

  public ResponseStatus getResponseStatus() {
    return responseStatus;
  }

  public Set<PipelinesProcessStatus.PipelinesStep.StepName> getSteps() {
    return steps;
  }

  public static class Builder {

    private ResponseStatus responseStatus;
    private Set<PipelinesProcessStatus.PipelinesStep.StepName> steps;

    public Builder setResponseStatus(ResponseStatus responseStatus) {
      this.responseStatus = responseStatus;
      return this;
    }

    public Builder setSteps(Set<PipelinesProcessStatus.PipelinesStep.StepName> steps) {
      this.steps = steps;
      return this;
    }

    public ReRunPipelineResponse build() {
      return new ReRunPipelineResponse(responseStatus, steps);
    }
  }
}
