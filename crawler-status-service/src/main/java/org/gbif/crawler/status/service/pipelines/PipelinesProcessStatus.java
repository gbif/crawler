package org.gbif.crawler.status.service.pipelines;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Base POJO model for the Pipelines monitoring service
 */
public class PipelinesProcessStatus implements Serializable {

  private static final long serialVersionUID = -3992826055732414678L;

  private long id;
  private String datasetKey;
  private String attempt;
  private String datasetTitle;
  private Set<PipelinesStep> steps = new TreeSet<>(Comparator.comparing(PipelinesStep::getStarted));

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getDatasetKey() {
    return datasetKey;
  }

  public void setDatasetKey(String datasetKey) {
    this.datasetKey = datasetKey;
  }

  public String getAttempt() {
    return attempt;
  }

  public void setAttempt(String attempt) {
    this.attempt = attempt;
  }

  public String getDatasetTitle() {
    return datasetTitle;
  }

  public void setDatasetTitle(String datasetTitle) {
    this.datasetTitle = datasetTitle;
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

    private long id;
    private final String name;
    private String runner;
    private LocalDateTime started;
    private LocalDateTime finished;
    private Status state;
    private String message;
    private Set<MetricInfo> metrics = new HashSet<>();

    public PipelinesStep(String name) {
      this.name = name;
    }

    public long getId() {
      return id;
    }

    public void setId(long id) {
      this.id = id;
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

    public String getName() {
      return name;
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

    public void setMetrics(Set<MetricInfo> metrics) {
      this.metrics = metrics;
    }

    public enum Status {
      RUNNING,
      FAILED,
      COMPLETED
    }
  }

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
  }

}
