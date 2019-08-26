package org.gbif.crawler.status.service.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/** Base POJO model for the Pipelines status service */
public class PipelinesProcessStatus implements Serializable {

  private static final long serialVersionUID = -3992826055732414678L;

  private long key;
  private UUID datasetKey;
  private int attempt;
  private String datasetTitle;
  private LocalDateTime created;
  private String createdBy;
  private Set<PipelinesStep> steps = new TreeSet<>(Comparator.comparing(PipelinesStep::getStarted));

  public long getKey() {
    return key;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public PipelinesProcessStatus setDatasetKey(UUID datasetKey) {
    this.datasetKey = datasetKey;
    return this;
  }

  public int getAttempt() {
    return attempt;
  }

  public PipelinesProcessStatus setAttempt(int attempt) {
    this.attempt = attempt;
    return this;
  }

  public String getDatasetTitle() {
    return datasetTitle;
  }

  public PipelinesProcessStatus setDatasetTitle(String datasetTitle) {
    this.datasetTitle = datasetTitle;
    return this;
  }

  public LocalDateTime getCreated() {
    return created;
  }

  public PipelinesProcessStatus setCreated(LocalDateTime created) {
    this.created = created;
    return this;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public PipelinesProcessStatus setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
    return this;
  }

  public Set<PipelinesStep> getSteps() {
    return steps;
  }

  public void setSteps(Set<PipelinesStep> steps) {
    this.steps = steps;
  }

  public void addStep(PipelinesStep step) {
    steps.add(step);
  }
}
