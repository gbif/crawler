package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.util.*;

import com.google.common.base.Strings;

/** Holds the search params for the {@link PipelinesRunningProcessSearchService}. */
public class SearchParams {

  private String datasetTitle;
  private UUID datasetKey;
  private Set<StepType> stepTypes = new HashSet<>();
  private Set<PipelineStep.Status> statuses = new HashSet<>();

  public Optional<String> getDatasetTitle() {
    return Optional.ofNullable(datasetTitle);
  }

  public void setDatasetTitle(String datasetTitle) {
    this.datasetTitle = datasetTitle;
  }

  public Optional<UUID> getDatasetKey() {
    return Optional.ofNullable(datasetKey);
  }

  public void setDatasetKey(UUID datasetKey) {
    this.datasetKey = datasetKey;
  }

  public Set<StepType> getStepTypes() {
    return stepTypes;
  }

  public void setStepTypes(Set<StepType> stepTypes) {
    this.stepTypes = stepTypes;
  }

  public Set<PipelineStep.Status> getStatuses() {
    return statuses;
  }

  public void setStatuses(Set<PipelineStep.Status> statuses) {
    this.statuses = statuses;
  }

  public boolean isEmpty() {
    return Strings.isNullOrEmpty(datasetTitle)
        && datasetKey == null
        && (stepTypes == null || stepTypes.isEmpty())
        && (statuses == null || statuses.isEmpty());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String datasetTitle;
    private UUID datasetKey;
    private Set<StepType> stepTypes = new HashSet<>();
    private Set<PipelineStep.Status> statuses = new HashSet<>();

    public Builder setDatasetTitle(String datasetTitle) {
      this.datasetTitle = datasetTitle;
      return this;
    }

    public Builder setDatasetKey(UUID datasetKey) {
      this.datasetKey = datasetKey;
      return this;
    }

    public Builder setStepTypes(List<StepType> stepTypes) {
      this.stepTypes = new HashSet<>(stepTypes);
      return this;
    }

    public Builder setStatuses(List<PipelineStep.Status> statuses) {
      this.statuses = new HashSet<>(statuses);
      return this;
    }

    public Builder addStepType(StepType stepType) {
      this.stepTypes.add(stepType);
      return this;
    }

    public Builder addStatus(PipelineStep.Status status) {
      this.statuses.add(status);
      return this;
    }

    public SearchParams build() {
      SearchParams params = new SearchParams();
      params.setDatasetTitle(datasetTitle);
      params.setDatasetKey(datasetKey);
      params.setStepTypes(stepTypes);
      params.setStatuses(statuses);

      return params;
    }
  }
}
