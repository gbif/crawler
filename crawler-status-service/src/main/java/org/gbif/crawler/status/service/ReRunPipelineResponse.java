package org.gbif.crawler.status.service;


import org.gbif.crawler.status.service.model.StepType;

import java.util.Set;

public class ReRunPipelineResponse {

  public enum ResponseStatus {
    OK,
    PIPELINE_IN_SUBMITTED,
    UNSUPPORTED_STEP,
    ERROR
  }


  private final ResponseStatus responseStatus;

  private final Set<StepType> steps;

  public ReRunPipelineResponse(ResponseStatus responseStatus, Set<StepType> steps) {
    this.responseStatus = responseStatus;
    this.steps = steps;
  }

  public ResponseStatus getResponseStatus() {
    return responseStatus;
  }

  public Set<StepType> getSteps() {
    return steps;
  }

  public static class Builder {

    private ResponseStatus responseStatus;
    private Set<StepType> step;

    public Builder setResponseStatus(ResponseStatus responseStatus) {
      this.responseStatus = responseStatus;
      return this;
    }

    public Builder setStep(Set<StepType> step) {
      this.step = step;
      return this;
    }

    public ReRunPipelineResponse build() {
      return new ReRunPipelineResponse(responseStatus, step);
    }
  }
}
