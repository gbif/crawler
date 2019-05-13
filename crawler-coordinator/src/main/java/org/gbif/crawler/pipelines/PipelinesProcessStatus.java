package org.gbif.crawler.pipelines;

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

  private static final long serialVersionUID = -3992826055732414677L;

  private final String crawlId;
  private Set<PipelinesStep> pipelinesSteps = new TreeSet<>(Comparator.comparing(PipelinesStep::getStartDateTime));
  private Set<MetricInfo> metricInfos = new HashSet<>();

  public PipelinesProcessStatus(String crawlId) {
    this.crawlId = crawlId;
  }

  public String getCrawlId() {
    return crawlId;
  }

  public Set<PipelinesStep> getPipelinesSteps() {
    return pipelinesSteps;
  }

  public Set<MetricInfo> getMetricInfos() {
    return metricInfos;
  }

  public void addStep(PipelinesStep step) {
    pipelinesSteps.add(step);
  }

  public void addMericInfo(MetricInfo metricInfo) {
    metricInfos.add(metricInfo);
  }

  public static class PipelinesStep implements Serializable {

    private static final long serialVersionUID = 460047082156621659L;

    private final String name;
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;
    private Status error = new Status();
    private Status successful = new Status();

    public PipelinesStep(String name) {
      this.name = name;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PipelinesStep that = (PipelinesStep) o;
      return Objects.equals(name, that.name) &&
          Objects.equals(startDateTime, that.startDateTime) &&
          Objects.equals(endDateTime, that.endDateTime);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, startDateTime, endDateTime);
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
