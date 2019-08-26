package org.gbif.crawler.status.service.pipelines;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;

/** Base POJO model for the Pipelines monitoring service */
public class PipelinesProcessStatus implements Serializable {

  private static final long serialVersionUID = -3992826055732414678L;

  private long key;
  private UUID datasetKey;
  private int attempt;
  private String datasetTitle;
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

  public Set<PipelinesStep> getSteps() {
    return steps;
  }

  public void setSteps(Set<PipelinesStep> steps) {
    this.steps = steps;
  }

  public void addStep(PipelinesStep step) {
    steps.add(step);
  }

  public static class PipelinesStep implements Serializable {

    private static final long serialVersionUID = 460047082156621661L;

    private long key;
    private StepName name;
    private String runner;
    private LocalDateTime started;
    private LocalDateTime finished;
    private Status state;
    private String message;
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

    public Optional<PipelinesStep> getStep() {
      if (started != null || finished != null) {
        started = started == null ? finished : started;
        return Optional.of(this);
      } else {
        return Optional.empty();
      }
    }

    public Set<MetricInfo> getMetrics() {
      return metrics;
    }

    public PipelinesStep setMetrics(Set<MetricInfo> metrics) {
      this.metrics = metrics;
      return this;
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
          && Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, name, runner, started, finished, state, message, metrics);
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
          .toString();
    }

    /** Enum to represent the status of a step. */
    public enum Status {
      RUNNING,
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
  }
}
