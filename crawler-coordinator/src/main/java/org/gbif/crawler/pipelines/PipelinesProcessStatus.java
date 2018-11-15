package org.gbif.crawler.pipelines;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Objects;

/**
 * Base POJO model for the Pipleines monitoring service
 */
public class PipelinesProcessStatus implements Serializable {

  private static final long serialVersionUID = -3992826055732414677L;

  private final String crawlId;
  private Set<PipelinesStep> pipelinesSteps = new TreeSet<>(Comparator.comparing(PipelinesStep::getIdx));

  public PipelinesProcessStatus(String crawlId) {
    this.crawlId = crawlId;
  }

  public String getCrawlId() {
    return crawlId;
  }

  public Set<PipelinesStep> getPipelinesSteps() {
    return pipelinesSteps;
  }

  public void addStep(PipelinesStep step) {
    pipelinesSteps.add(step);
  }

  public static class PipelinesStep implements Serializable {

    private static final long serialVersionUID = 460047082156621659L;

    private final int idx;
    private final String name;
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;
    private Status error = new Status();
    private Status successful = new Status();

    public PipelinesStep(int idx, String name) {
      this.idx = idx;
      this.name = name;
    }

    public int getIdx() {
      return idx;
    }

    public String getName() {
      return name;
    }

    public LocalDateTime getStartDateTime() {
      return startDateTime;
    }

    public PipelinesStep setStartDateTime(LocalDateTime startDate) {
      this.startDateTime = startDate;
      return this;
    }

    public LocalDateTime getEndDateTime() {
      return endDateTime;
    }

    public PipelinesStep setEndDateTime(LocalDateTime endDate) {
      this.endDateTime = endDate;
      return this;
    }

    public Optional<PipelinesStep> getStep() {
      return startDateTime != null || endDateTime != null ? Optional.of(this) : Optional.empty();
    }

    public Status getError() {
      return error;
    }

    public PipelinesStep setError(Status error) {
      this.error = error;
      return this;
    }

    public Status getSuccessful() {
      return successful;
    }

    public PipelinesStep setSuccessful(Status successful) {
      this.successful = successful;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PipelinesStep step = (PipelinesStep) o;
      return idx == step.idx && Objects.equal(name, step.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(idx, name);
    }

    public static class Status implements Serializable {

      private static final long serialVersionUID = 1827285369622224859L;

      private boolean availability = false;
      private String message = "";

      public boolean isAvailability() {
        return availability;
      }

      public Status setAvailability(boolean availability) {
        this.availability = availability;
        return this;
      }

      public String getMessage() {
        return message;
      }

      public Status setMessage(String message) {
        this.message = message;
        return this;
      }
    }
  }

}
