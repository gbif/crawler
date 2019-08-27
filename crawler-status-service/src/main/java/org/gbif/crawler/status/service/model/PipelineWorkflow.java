package org.gbif.crawler.status.service.model;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class PipelineWorkflow implements Serializable {

  private UUID datasetKey;
  private int attempt;

  // there should be only one initial step
  private WorkflowStep initialStep;

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public void setDatasetKey(UUID datasetKey) {
    this.datasetKey = datasetKey;
  }

  public int getAttempt() {
    return attempt;
  }

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public WorkflowStep getInitialStep() {
    return initialStep;
  }

  public void setInitialStep(WorkflowStep initialStep) {
    this.initialStep = initialStep;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PipelineWorkflow that = (PipelineWorkflow) o;
    return attempt == that.attempt && Objects.equals(datasetKey, that.datasetKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetKey, attempt);
  }
}
