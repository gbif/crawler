package org.gbif.crawler;

import org.gbif.api.model.registry.Endpoint;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * This package-visible class consolidates all metrics collected by the {@link CrawlerCoordinatorServiceImpl} class.
 */
class CrawlerCoordinatorServiceMetrics {

  private static final String FAIL_COUNTER_NAME = name(CrawlerCoordinatorService.class, "schedules", "failed");
  private final MetricRegistry registry = new MetricRegistry();
  private final Counter successfulSchedules =
    registry.counter(name(CrawlerCoordinatorService.class, "schedules", "successful"));
  private final Counter unsuccessfulSchedules = registry.counter(FAIL_COUNTER_NAME);
  private final Counter alreadyScheduledDatasets = registry.counter(name(FAIL_COUNTER_NAME, "alreadyScheduled"));
  private final Counter noValidEndpoints = registry.counter(name(FAIL_COUNTER_NAME, "noValidEndpoints"));
  private final Counter digirRequests = registry.counter(name(CrawlerCoordinatorService.class, "requests", "digir"));
  private final Counter biocaseRequests =
    registry.counter(name(CrawlerCoordinatorService.class, "requests", "biocase"));
  private final Counter tapirRequests = registry.counter(name(CrawlerCoordinatorService.class, "requests", "tapir"));
  /* These two fields are used to give a quick "status" to anyone interested. I modeled it after the Hadoop status
     messages which have proven to be useful in the past. This UUID is updated every time a new dataset UUID was
     successfully enqueued. */
  private final AtomicReference<UUID> lastUuid = new AtomicReference<UUID>();
  private final Gauge<UUID> lastSuccessfulDatasetKey =
    registry.register(name(CrawlerCoordinatorService.class, "lastSuccessfulDatasetKey"), new Gauge<UUID>() {
      @Override
      public UUID getValue() {
        return lastUuid.get();
      }
    });
  private final Timer crawls = registry.timer(name(CrawlerCoordinatorService.class, "crawls"));
  private Timer.Context timerContext;

  public void alreadyScheduled() {
    alreadyScheduledDatasets.inc();
  }

  public void noValidEndpoint() {
    noValidEndpoints.inc();
  }

  public void registerCrawl(Endpoint endpoint) {
    switch (endpoint.getType()) {
      case DIGIR:
      case DIGIR_MANIS:
        digirRequests.inc();
        break;
      case TAPIR:
        tapirRequests.inc();
        break;
      case BIOCASE:
        biocaseRequests.inc();
        break;
    }
  }

  public void successfulSchedule(UUID datasetKey) {
    lastUuid.set(datasetKey);
    successfulSchedules.inc();
  }

  public void timerStart() {
    timerContext = crawls.time();
  }

  public void timerStop() {
    timerContext.stop();
  }

  public void unsuccessfulSchedule() {
    unsuccessfulSchedules.inc();
  }

}
