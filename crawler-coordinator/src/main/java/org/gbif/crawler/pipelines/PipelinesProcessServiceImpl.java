package org.gbif.crawler.pipelines;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.crawler.DatasetProcessServiceImpl;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.pipelines.PipelinesProcessStatus.PipelinesStep;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.google.common.base.Charsets;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.CrawlerNodePaths.buildPath;
import static org.gbif.crawler.constants.PipelinesNodePaths.ALL_STEPS;
import static org.gbif.crawler.constants.PipelinesNodePaths.PIPELINES_ROOT;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Pipelines monitoring service collects all necessary information from Zookeeper
 */
public class PipelinesProcessServiceImpl implements PipelinesProcessService {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetProcessServiceImpl.class);

  private final CuratorFramework curator;
  private final Executor executor;

  /**
   * Creates a CrawlerMetricsService. Responsible for interacting with a ZooKeeper instance in a read-only fashion.
   *
   * @param curator  to access ZooKeeper
   * @param executor to run the thread pool
   */
  @Inject
  public PipelinesProcessServiceImpl(CuratorFramework curator, Executor executor) {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.executor = checkNotNull(executor, "executor can't be null");
  }

  /**
   * Reads all monitoring information from Zookeeper pipelines root path
   */
  @Override
  public Set<PipelinesProcessStatus> getRunningPipelinesProcesses() {
    Set<PipelinesProcessStatus> set = new HashSet<>();
    try {

      String path = buildPath(PIPELINES_ROOT);

      if (!checkExists(path)) {
        return Collections.emptySet();
      }

      // Reads all nodes in async mode
      CompletableFuture[] futures = curator.getChildren()
        .forPath(path)
        .stream()
        .map(id -> CompletableFuture.runAsync(() -> set.add(getRunningPipelinesProcess(id)), executor))
        .toArray(CompletableFuture[]::new);

      // Waits all threads
      CompletableFuture.allOf(futures).get();

    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException ex) {
      LOG.warn("Caught exception trying to retrieve dataset", ex.getCause());
    } catch (Exception ex) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }

    return set;
  }

  /**
   * Reads monitoring information from Zookeeper by crawlId node path
   *
   * @param crawlId path to dataset info
   */
  @Override
  public PipelinesProcessStatus getRunningPipelinesProcess(String crawlId) {
    checkNotNull(crawlId, "crawlId can't be null");

    try {
      // Check if dataset is actually being processed right now
      if (!checkExists(getPipelinesInfoPath(crawlId))) {
        return new PipelinesProcessStatus(crawlId);
      }
      // Here we're trying to load all information from Zookeeper into the DatasetProcessStatus object
      PipelinesProcessStatus status = new PipelinesProcessStatus(crawlId);
      Supplier<Integer> nextIdxFn = () -> status.getPipelinesSteps().size() + 1;

      // ALL_STEPS - static set of all pipelines steps: DWCA_TO_AVRO, VERBATIM_TO_INTERPRETED and etc.
      for (String path: ALL_STEPS) {
        PipelinesStep step = new PipelinesStep(nextIdxFn.get(), path);

        getAsDate(crawlId, Fn.START_DATE.apply(path)).ifPresent(step::setStartDateTime);
        getAsDate(crawlId, Fn.END_DATE.apply(path)).ifPresent(step::setEndDateTime);

        getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(path)).ifPresent(step.getError()::setAvailability);
        getAsString(crawlId, Fn.ERROR_MESSAGE.apply(path)).ifPresent(step.getError()::setMessage);

        getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(path)).ifPresent(step.getSuccessful()::setAvailability);
        getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(path)).ifPresent(step.getSuccessful()::setMessage);

        step.getStep().ifPresent(status::addStep);
      }
      return status;
    } catch (Exception ex) {
      LOG.error("crawlId {}", crawlId, ex.getCause());
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  @Override
  public void deleteRunningPipelinesProcess(String crawlId) {
    try {
      String path = getPipelinesInfoPath(crawlId);
      if (checkExists(path)) {
        curator.delete().deletingChildrenIfNeeded().forPath(path);
      }
    } catch (Exception ex) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  @Override
  public Set<PipelinesProcessStatus> getPipelinesProcessesByDatasetKey(String datasetKey) {
    Set<PipelinesProcessStatus> set = new HashSet<>();
    try {

      String path = buildPath(PIPELINES_ROOT);

      if (!checkExists(path)) {
        return Collections.emptySet();
      }

      // Reads all nodes in async mode
      CompletableFuture[] futures = curator.getChildren()
        .forPath(path)
        .stream()
        .filter(node -> node.startsWith(datasetKey))
        .map(id -> CompletableFuture.runAsync(() -> set.add(getRunningPipelinesProcess(id)), executor))
        .toArray(CompletableFuture[]::new);

      // Waits all threads
      CompletableFuture.allOf(futures).get();

    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException ex) {
      LOG.warn("Caught exception trying to retrieve dataset", ex.getCause());
    } catch (Exception ex) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }

    return set;
  }

  /**
   * Check exists a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private boolean checkExists(String crawlId) throws Exception {
    return curator.checkExists().forPath(crawlId) != null;
  }

  /**
   * Read value from Zookeeper as a {@link String}
   */
  private Optional<String> getAsString(String crawlId, String path) throws Exception {
    String infoPath = getPipelinesInfoPath(crawlId, path);
    if (checkExists(infoPath)) {
      byte[] responseData = curator.getData().forPath(infoPath);
      if (responseData != null) {
        return Optional.of(new String(responseData, Charsets.UTF_8));
      }
    }
    return Optional.empty();
  }

  /**
   * Read value from Zookeeper as a {@link LocalDateTime}
   */
  private Optional<LocalDateTime> getAsDate(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(x -> LocalDateTime.parse(x, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (DateTimeParseException ex) {
      LOG.warn("Date was not parsed successfully: [{}]: {}", data.orElse(crawlId), ex);
      return Optional.empty();
    }
  }

  /**
   * Read value from Zookeeper as a {@link Boolean}
   */
  private Optional<Boolean> getAsBoolean(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(Boolean::parseBoolean);
    } catch (DateTimeParseException ex) {
      LOG.warn("Boolean was not parsed successfully: [{}]: {}", data.orElse(crawlId), ex);
      return Optional.empty();
    }
  }
}
