package org.gbif.crawler.status.service.model;

import org.gbif.api.model.registry.LenientEquals;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;

/** Models a step in pipelines. */
public class PipelinesStep implements LenientEquals<PipelinesStep>, Serializable {

  private static final long serialVersionUID = 460047082156621661L;

  private long key;
  private StepName name;
  private String runner;
  private LocalDateTime started;
  private LocalDateTime finished;
  private Status state;
  private String message;
  private String rerunReason;
  private LocalDateTime created;
  private String createdBy;
  private Set<MetricInfo> metrics = new HashSet<>();

  public long getKey() {
    return key;
  }

  public StepName getName() {
    return name;
  }

  public PipelinesStep setName(StepName name) {
    this.name = name;
    return this;
  }

  public String getRunner() {
    return runner;
  }

  public PipelinesStep setRunner(String runner) {
    this.runner = runner;
    return this;
  }

  public LocalDateTime getStarted() {
    return started;
  }

  public PipelinesStep setStarted(LocalDateTime started) {
    this.started = started;
    return this;
  }

  public LocalDateTime getFinished() {
    return finished;
  }

  public PipelinesStep setFinished(LocalDateTime finished) {
    this.finished = finished;
    return this;
  }

  public Status getState() {
    return state;
  }

  public PipelinesStep setState(Status state) {
    this.state = state;
    return this;
  }

  public String getMessage() {
    return message;
  }

  public PipelinesStep setMessage(String message) {
    this.message = message;
    return this;
  }

  public String getRerunReason() {
    return rerunReason;
  }

  public PipelinesStep setRerunReason(String rerunReason) {
    this.rerunReason = rerunReason;
    return this;
  }

  public LocalDateTime getCreated() {
    return created;
  }

  public PipelinesStep setCreated(LocalDateTime created) {
    this.created = created;
    return this;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public PipelinesStep setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
    return this;
  }

  public Set<MetricInfo> getMetrics() {
    return metrics;
  }

  public PipelinesStep setMetrics(Set<MetricInfo> metrics) {
    this.metrics = metrics;
    return this;
  }

  /** Enum to represent the status of a step. */
  public enum Status {
    SUBMITTED,
    FAILED,
    COMPLETED
  }

  /** Enum to represent the step names. */
  public enum StepName {
    DWCA_TO_VERBATIM,
    XML_TO_VERBATIM,
    ABCD_TO_VERBATIM,
    VERBATIM_TO_INTERPRETED,
    INTERPRETED_TO_INDEX,
    HIVE_VIEW
  }

  /** Inner class to store metrics. */
  public static class MetricInfo implements Serializable {

    private static final long serialVersionUID = 1872427841009786709L;

    private String name;
    private String value;

    public MetricInfo(String name, String value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MetricInfo that = (MetricInfo) o;
      return name.equals(that.name) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", MetricInfo.class.getSimpleName() + "[", "]")
          .add("name='" + name + "'")
          .add("value='" + value + "'")
          .toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PipelinesStep that = (PipelinesStep) o;
    return key == that.key
        && Objects.equals(name, that.name)
        && Objects.equals(runner, that.runner)
        && Objects.equals(started, that.started)
        && Objects.equals(finished, that.finished)
        && state == that.state
        && Objects.equals(message, that.message)
        && Objects.equals(metrics, that.metrics)
        && Objects.equals(rerunReason, that.rerunReason)
        && Objects.equals(created, that.created)
        && Objects.equals(createdBy, that.createdBy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        key,
        name,
        runner,
        started,
        finished,
        state,
        message,
        metrics,
        rerunReason,
        created,
        createdBy);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PipelinesStep.class.getSimpleName() + "[", "]")
        .add("key=" + key)
        .add("name='" + name + "'")
        .add("runner='" + runner + "'")
        .add("started=" + started)
        .add("finished=" + finished)
        .add("state=" + state)
        .add("message='" + message + "'")
        .add("metrics=" + metrics)
        .add("rerunReason=" + rerunReason)
        .add("created=" + created)
        .add("createdBy=" + createdBy)
        .toString();
  }

  @Override
  public boolean lenientEquals(PipelinesStep other) {
    if (this == other) return true;
    return Objects.equals(name, other.name)
        && Objects.equals(runner, other.runner)
        && Objects.equals(started, other.started)
        && Objects.equals(finished, other.finished)
        && state == other.state
        && Objects.equals(message, other.message)
        && Objects.equals(metrics, other.metrics)
        && Objects.equals(rerunReason, other.rerunReason);
  }
}
