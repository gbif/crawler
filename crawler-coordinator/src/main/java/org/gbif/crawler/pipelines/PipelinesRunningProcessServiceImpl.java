package org.gbif.crawler.pipelines;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.pipelines.*;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.pipelines.search.PipelinesRunningProcessSearchService;
import org.gbif.crawler.pipelines.search.SearchParams;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.CacheEntry;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.model.pipelines.PipelineProcess.PIPELINE_PROCESS_BY_LATEST_STEP_RUNNING_ASC;
import static org.gbif.api.model.pipelines.PipelineStep.Status;
import static org.gbif.crawler.constants.PipelinesNodePaths.DELIMITER;
import static org.gbif.crawler.constants.PipelinesNodePaths.PIPELINES_ROOT;
import static org.gbif.crawler.constants.PipelinesNodePaths.SIZE;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.INITIALIZED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_ADDED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_REMOVED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_UPDATED;

/** Pipelines monitoring service collects all necessary information from Zookeeper */
public class PipelinesRunningProcessServiceImpl implements PipelinesRunningProcessService {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelinesRunningProcessServiceImpl.class);

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
  private static final BiFunction<UUID, Integer, String> CRAWL_ID_GENERATOR =
      (datasetKey, attempt) -> datasetKey + "_" + attempt;

  private final CuratorFramework curator;
  private final DatasetService datasetService;
  private final PipelinesRunningProcessSearchService searchService;
  private final Cache<String, PipelineProcess> processCache =
      Cache2kBuilder.of(String.class, PipelineProcess.class)
          .entryCapacity(Long.MAX_VALUE)
          .suppressExceptions(false)
          .eternal(true)
          .build();

  /**
   * Creates a CrawlerMetricsService. Responsible for interacting with a ZooKeeper instance in a
   * read-only fashion.
   *
   * @param curator to access ZooKeeper
   */
  @Inject
  public PipelinesRunningProcessServiceImpl(CuratorFramework curator, DatasetService datasetService)
      throws Exception {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.datasetService = datasetService;
    this.searchService = new PipelinesRunningProcessSearchService();
    Runtime.getRuntime().addShutdownHook(new Thread(searchService::close));
    setupTreeCache();
  }

  private void setupTreeCache() throws Exception {
    TreeCache cache =
        TreeCache.newBuilder(curator, CrawlerNodePaths.buildPath(PIPELINES_ROOT))
            .setCacheData(false)
            .build();
    cache.start();

    Function<String, Optional<String>> crawlIdPath =
        path -> {
          if (path.contains("lock")) {
            // ignoring lock events
            return Optional.empty();
          }

          String[] paths = path.substring(path.indexOf(PIPELINES_ROOT)).split(DELIMITER);
          if (paths.length > 1) {
            return Optional.of(paths[1]);
          }
          return Optional.empty();
        };

    TreeCacheListener listener =
        (curatorClient, event) -> {
          // we ignore the changes in the size node when adding or updating
          if (event.getType() == NODE_ADDED && !event.getData().getPath().contains(SIZE)) {
            crawlIdPath
                .apply(event.getData().getPath())
                .ifPresent(
                    path ->
                        loadRunningPipelineProcess(path)
                            .ifPresent(
                                process -> {
                                  processCache.put(path, process);
                                  // since some added events are actually updates in the same crawl
                                  // we update always the index to avoid duplicates
                                  searchService.update(process);
                                }));
          } else if (event.getType() == NODE_UPDATED) {
            crawlIdPath
                .apply(event.getData().getPath())
                .ifPresent(
                    path -> {
                      if (!processCache.containsKey(path)) {
                        // if already deleted we don't even create the PipelineProcess object
                        return;
                      }
                      loadRunningPipelineProcess(path)
                          .ifPresent(
                              process -> {
                                processCache.replace(path, process);
                                searchService.update(process);
                              });
                    });
          } else if (event.getType() == NODE_REMOVED) {
            crawlIdPath
                .apply(event.getData().getPath())
                .ifPresent(
                    path -> {
                      processCache.remove(path);
                      searchService.delete(path);
                    });
          } else if (event.getType() == INITIALIZED) {
            LOG.info("ZK TreeCache initialized for pipelines");
          }
        };
    cache.getListenable().addListener(listener);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  cache.close();
                  processCache.clearAndClose();
                }));
  }

  @Override
  public Set<PipelineProcess> getPipelineProcesses() {
    return getPipelineProcesses(null);
  }

  @Override
  public Set<PipelineProcess> getPipelineProcesses(UUID datasetKey) {
    return StreamSupport.stream(processCache.entries().spliterator(), true)
        .filter(node -> datasetKey == null || node.getKey().startsWith(datasetKey.toString()))
        .map(CacheEntry::getValue)
        .collect(
            Collectors.toCollection(
                () -> new TreeSet<>(PIPELINE_PROCESS_BY_LATEST_STEP_RUNNING_ASC.reversed())));
  }

  @Override
  public PipelineProcess getPipelineProcess(UUID datasetKey, int attempt) {
    return processCache.get(CRAWL_ID_GENERATOR.apply(datasetKey, attempt));
  }

  @Override
  public void deletePipelineProcess(UUID datasetKey, int attempt) {
    deleteZkPipelinesNode(CRAWL_ID_GENERATOR.apply(datasetKey, attempt));
  }

  /** Removes pipelines root Zookeeper path */
  @Override
  public void deleteAllPipelineProcess() {
    deleteZkPipelinesNode(null);
  }

  @Override
  public PipelineProcessSearchResult search(
      @Nullable String datasetTitle,
      @Nullable UUID datasetKey,
      @Nullable List<Status> stepStatuses,
      @Nullable List<StepType> stepTypes,
      int offset,
      int limit) {

    SearchParams searchParams =
        SearchParams.newBuilder()
            .setDatasetTitle(datasetTitle)
            .setDatasetKey(datasetKey)
            .setStepTypes(stepTypes)
            .setStatuses(stepStatuses)
            .build();

    // if there's no params get all processes
    if (searchParams.isEmpty()) {
      Set<PipelineProcess> allProcesses =
          StreamSupport.stream(processCache.entries().spliterator(), true)
              .map(CacheEntry::getValue)
              .sorted(PIPELINE_PROCESS_BY_LATEST_STEP_RUNNING_ASC.reversed())
              .skip(offset)
              .limit(limit)
              .collect(
                  Collectors.toCollection(
                      () -> new TreeSet<>(PIPELINE_PROCESS_BY_LATEST_STEP_RUNNING_ASC.reversed())));

      return new PipelineProcessSearchResult(allProcesses.size(), allProcesses);
    }

    // search by params
    List<String> hits = searchService.search(searchParams);
    Set<PipelineProcess> processes =
        hits.stream()
            .map(processCache::get)
            .sorted(PIPELINE_PROCESS_BY_LATEST_STEP_RUNNING_ASC.reversed())
            .skip(offset)
            .limit(limit)
            .collect(
                Collectors.toCollection(
                    () -> new TreeSet<>(PIPELINE_PROCESS_BY_LATEST_STEP_RUNNING_ASC.reversed())));

    return new PipelineProcessSearchResult(processes.size(), processes);
  }

  private void deleteZkPipelinesNode(String crawlId) {
    try {
      String path = getPipelinesInfoPath(crawlId);
      if (checkExists(path)) {
        curator.delete().deletingChildrenIfNeeded().forPath(path);
      }
    } catch (Exception ex) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  /**
   * Reads monitoring information from Zookeeper by crawlId node path
   *
   * @param crawlId path to dataset info
   */
  private Optional<PipelineProcess> loadRunningPipelineProcess(String crawlId) {
    checkNotNull(crawlId, "crawlId can't be null");

    try {
      String[] ids = crawlId.split("_");

      if (!checkExists(getPipelinesInfoPath(crawlId))) {
        return Optional.empty();
      }
      // Here we're trying to load all information from Zookeeper into a PipelineProcess object
      PipelineProcess process =
          new PipelineProcess()
              .setDatasetKey(UUID.fromString(ids[0]))
              .setAttempt(Integer.parseInt(ids[1]));

      // ALL_STEPS - static set of all pipelines steps: DWCA_TO_AVRO, VERBATIM_TO_INTERPRETED and
      // etc.
      getExecutions(crawlId).forEach(process::addExecution);

      if (process.getExecutions().isEmpty()) {
        return Optional.empty();
      }

      setDatasetTitle(process);

      return Optional.of(process);
    } catch (Exception ex) {
      LOG.error("crawlId {}", crawlId, ex.getCause());
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  private void setDatasetTitle(PipelineProcess process) {
    if (process != null && process.getDatasetKey() != null) {
      Dataset dataset = datasetService.get(process.getDatasetKey());
      if (dataset != null) {
        process.setDatasetTitle(dataset.getTitle());
      }
    }
  }

  /** Gets step info from ZK */
  private Set<PipelineExecution> getExecutions(String crawlId) {
    Map<Long, PipelineExecution> executionsMap = new HashMap<>();

    for (StepType stepType : StepType.values()) {
      PipelineStep step = new PipelineStep().setType(stepType);

      try {
        Optional<String> msg = getAsString(crawlId, Fn.MQ_MESSAGE.apply(stepType.getLabel()));
        Optional<LocalDateTime> startDateOpt =
            getAsDate(crawlId, Fn.START_DATE.apply(stepType.getLabel()));
        if (!msg.isPresent() || !startDateOpt.isPresent()) {
          // we skip steps without msg or start date. Without msg we can't assign them to an
          // execution
          continue;
        }

        // set fields
        step.setMessage(msg.get());
        step.setStarted(startDateOpt.get());

        // end date
        Optional<LocalDateTime> endDateOpt =
            getAsDate(crawlId, Fn.END_DATE.apply(stepType.getLabel()));
        endDateOpt.ifPresent(step::setFinished);

        // runner
        getAsString(crawlId, Fn.RUNNER.apply(stepType.getLabel()))
            .ifPresent(r -> step.setRunner(StepRunner.valueOf(r)));

        // state
        Optional<Boolean> isErrorOpt =
            getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(stepType.getLabel()));
        Optional<Boolean> isSuccessful =
            getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(stepType.getLabel()));
        if (isErrorOpt.isPresent()) {
          step.setState(Status.FAILED);
        } else if (isSuccessful.isPresent()) {
          step.setState(Status.COMPLETED);
        } else if (!endDateOpt.isPresent()) {
          step.setState(Status.RUNNING);
        }

        // assign the step to an execution
        Long executionId = OBJECT_MAPPER.readTree(step.getMessage()).get("executionId").asLong();
        PipelineExecution execution =
            executionsMap.computeIfAbsent(
                executionId,
                id -> new PipelineExecution().setKey(id));
        execution.addStep(step);
      } catch (Exception ex) {
        LOG.warn(ex.getMessage(), ex);
        // we skip this step
      }
    }

    return new HashSet<>(executionsMap.values());
  }

  /**
   * Check exists a Zookeeper monitoring root node by crawlId
   *
   * @param path root node path
   */
  private boolean checkExists(String path) throws Exception {
    return curator.checkExists().forPath(path) != null;
  }

  /** Read value from Zookeeper as a {@link String} */
  private Optional<String> getAsString(String crawlId, String path) throws Exception {
    String infoPath = getPipelinesInfoPath(crawlId, path);
    if (checkExists(infoPath)) {
      byte[] responseData = curator.getData().forPath(infoPath);
      if (responseData != null) {
        return Optional.of(new String(responseData, StandardCharsets.UTF_8));
      }
    }
    return Optional.empty();
  }

  /** Read value from Zookeeper as a {@link LocalDateTime} */
  private Optional<LocalDateTime> getAsDate(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(x -> LocalDateTime.parse(x, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (DateTimeParseException ex) {
      LOG.warn("Date was not parsed successfully: [{}]: {}", data.orElse(crawlId), ex);
      return Optional.empty();
    }
  }

  /** Read value from Zookeeper as a {@link Boolean} */
  private Optional<Boolean> getAsBoolean(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(Boolean::parseBoolean);
    } catch (DateTimeParseException ex) {
      LOG.warn("Boolean was not parsed successfully: [{}]: {}", data.orElse(crawlId), ex);
      return Optional.empty();
    }
  }

  /** Search results. */
  public static final class PipelineProcessSearchResult {

    private final long totalHits;

    private final Set<PipelineProcess> results;

    PipelineProcessSearchResult(long totalHits, Set<PipelineProcess> results) {
      this.totalHits = totalHits;
      this.results = results;
    }

    /** @return the total/global number of results */
    public long getTotalHits() {
      return totalHits;
    }

    /** @return PipelineProcess search results */
    public Set<PipelineProcess> getResults() {
      return results;
    }
  }
}
