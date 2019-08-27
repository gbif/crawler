package org.gbif.crawler.status.service.model;

import java.io.Serializable;
import java.util.*;

public class WorkflowStep implements Serializable {

  private StepType stepType;

  private PipelineStep lastStep;

  private TreeSet<PipelineStep> allSteps =
      new TreeSet<>(Comparator.comparing(PipelineStep::getStarted).reversed());

  private List<WorkflowStep> nextSteps = new ArrayList<>();

  public StepType getStepType() {
    return stepType;
  }

  public void setStepType(StepType stepType) {
    this.stepType = stepType;
  }

  public PipelineStep getLastStep() {
    return lastStep;
  }

  public void setLastStep(PipelineStep lastStep) {
    this.lastStep = lastStep;
  }

  public TreeSet<PipelineStep> getAllSteps() {
    return allSteps;
  }

  public void setAllSteps(TreeSet<PipelineStep> allSteps) {
    this.allSteps = allSteps;
  }

  public List<WorkflowStep> getNextSteps() {
    return nextSteps;
  }

  public void setNextSteps(List<WorkflowStep> nextSteps) {
    this.nextSteps = nextSteps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkflowStep that = (WorkflowStep) o;
    return stepType == that.stepType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stepType);
  }
}
