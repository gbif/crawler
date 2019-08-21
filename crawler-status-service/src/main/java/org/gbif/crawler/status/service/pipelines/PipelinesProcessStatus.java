package org.gbif.crawler.status.service.pipelines;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Base POJO model for the Pipelines monitoring service
 */
public class PipelinesProcessStatus implements Serializable {

  private static final long serialVersionUID = -3992826055732414678L;

  private String datasetKey;
  private String attempt;
  private String datasetTitle;
  private Set<PipelinesStep> steps = new TreeSet<>(Comparator.comparing(PipelinesStep::getStarted));

  public Set<PipelinesStep> getSteps() {
    return steps;
  }

  public String getDatasetKey() {
    return datasetKey;
  }

  public PipelinesProcessStatus setDatasetKey(String datasetKey) {
    this.datasetKey = datasetKey;
    return this;
  }

  public String getAttempt() {
    return attempt;
  }

  public PipelinesProcessStatus setAttempt(String attempt) {
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

  public void addStep(PipelinesStep step) {
    steps.add(step);
  }

  public static class PipelinesStep implements Serializable {

    private static final long serialVersionUID = 460047082156621661L;

    private final String name;
    private String runner;
    private String started;
    private String finished;
    private Status state;
    private String message;
    private Map<String, String> metrics = new HashMap();

    public PipelinesStep(String name) {
      this.name = name;
    }

    public String getRunner() {
      return runner;
    }

    public PipelinesStep setRunner(String runner) {
      this.runner = runner;
      return this;
    }

    public String getStarted() {
      return started;
    }

    public PipelinesStep setStarted(String started) {
      this.started = started;
      return this;
    }

    public String getFinished() {
      return finished;
    }

    public PipelinesStep setFinished(String finished) {
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

    public Map<String, String> getMetrics() {
      return metrics;
    }

    public void setMetrics(Map<String, String> metrics) {
      this.metrics = metrics;
    }

    public enum Status {
      RUNNING,
      FAILED,
      COMPLETED
    }
  }

}
