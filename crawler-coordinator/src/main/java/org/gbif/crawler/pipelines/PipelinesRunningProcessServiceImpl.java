package org.gbif.crawler.pipelines;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.pipelines.search.PipelinesRunningProcessSearchService;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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

import static org.gbif.api.model.pipelines.PipelineProcess.PIPELINE_PROCESS_BY_LATEST_STEP_ASC;
import static org.gbif.api.model.pipelines.PipelineStep.STEPS_BY_START_AND_FINISH_ASC;
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
  private static final String FILTER_NAME = "org.gbif.pipelines.transforms.";
  private static final DecimalFormat DF = new DecimalFormat("0");
  private static final BiFunction<UUID, Integer, String> CRAWL_ID_GENERATOR =
      (datasetKey, attempt) -> datasetKey + "_" + attempt;

  private final CuratorFramework curator;
  private final RestHighLevelClient client;
  private final String envPrefix;
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
  public PipelinesRunningProcessServiceImpl(
      CuratorFramework curator,
      RestHighLevelClient client,
      DatasetService datasetService,
      @Named("pipelines.envPrefix") String envPrefix) throws Exception {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.client = client;
    this.datasetService = datasetService;
    this.envPrefix = envPrefix;
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    this.searchService = new PipelinesRunningProcessSearchService(tmpDir.getPath());
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
          if ((event.getType() == NODE_ADDED || event.getType() == NODE_UPDATED)) {
            crawlIdPath
                .apply(event.getData().getPath())
                .flatMap(this::loadRunningPipelineProcess)
                .ifPresent(searchService::index);
          } else if (event.getType() == NODE_REMOVED) {
            crawlIdPath.apply(event.getData().getPath()).ifPresent(searchService::delete);
          } else if (event.getType() == INITIALIZED) {
            LOG.info("ZK TreeCache initialized for pipelines");
          }
        };
    cache.getListenable().addListener(listener);

    Runtime.getRuntime().addShutdownHook(new Thread(cache::close));
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
                () -> new TreeSet<>(PIPELINE_PROCESS_BY_LATEST_STEP_ASC.reversed())));
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
      @Nullable Status stepStatus,
      @Nullable StepType stepType,
      int pageNumber,
      int pageSize) {

    Function<List<String>, PipelineProcessSearchResult> hitsToResult =
        hits -> {
          Set<PipelineProcess> processes =
              hits.stream().map(processCache::get).collect(Collectors.toSet());

          return new PipelineProcessSearchResult(processes.size(), processes);
        };

    if (!Strings.isNullOrEmpty(datasetTitle)) {
      return hitsToResult.apply(
          searchService.searchByDatasetTitle(datasetTitle, pageNumber, pageSize));
    }

    if (stepStatus != null && stepType != null) {
      return hitsToResult.apply(
          searchService.searchByStepStatus(stepType, stepStatus, pageNumber, pageSize));
    }

    if (stepStatus != null) {
      return hitsToResult.apply(searchService.searchByStatus(stepStatus, pageNumber, pageSize));
    }

    if (stepType != null) {
      return hitsToResult.apply(searchService.searchByStep(stepType, pageNumber, pageSize));
    }

    // if there's no params get all processes
    Set<PipelineProcess> allProcesses =
        StreamSupport.stream(processCache.entries().spliterator(), true)
            .skip(pageNumber)
            .limit(pageSize)
            .map(CacheEntry::getValue)
            .collect(
                Collectors.toCollection(
                    () -> new TreeSet<>(PIPELINE_PROCESS_BY_LATEST_STEP_ASC.reversed())));

    return new PipelineProcessSearchResult(allProcesses.size(), allProcesses);
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

      // Check if dataset is actually being processed right now
      if (!checkExists(getPipelinesInfoPath(crawlId))) {
        return Optional.of(
            new PipelineProcess()
                .setDatasetKey(UUID.fromString(ids[0]))
                .setAttempt(Integer.parseInt(ids[1])));
      }
      // Here we're trying to load all information from Zookeeper into a PipelineProcess object
      PipelineProcess process =
          new PipelineProcess()
              .setDatasetKey(UUID.fromString(ids[0]))
              .setAttempt(Integer.parseInt(ids[1]));

      // ALL_STEPS - static set of all pipelines steps: DWCA_TO_AVRO, VERBATIM_TO_INTERPRETED and
      // etc.
      getStepInfo(crawlId).stream().filter(s -> s.getStarted() != null).forEach(process::addStep);

      if (process.getSteps().isEmpty()) {
        return Optional.empty();
      }

      addNumberRecords(process, crawlId);
      setDatasetTitle(process);

      return Optional.of(process);
    } catch (Exception ex) {
      LOG.error("crawlId {}", crawlId, ex.getCause());
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  private void addNumberRecords(PipelineProcess status, String crawlId) {
    // get number of records
    status.getSteps().stream()
        .filter(s -> s.getType().getExecutionOrder() == 1)
        .max(STEPS_BY_START_AND_FINISH_ASC)
        .ifPresent(
            s -> {
              try {
                Optional<String> msg =
                    getAsString(crawlId, Fn.MQ_MESSAGE.apply(s.getType().getLabel()));

                if (!msg.isPresent()) {
                  return;
                }

                if (s.getType() == StepType.DWCA_TO_VERBATIM) {
                  status.setNumberRecords(
                      OBJECT_MAPPER
                          .readValue(msg.get(), PipelinesDwcaMessage.class)
                          .getValidationReport()
                          .getOccurrenceReport()
                          .getCheckedRecords());
                } else if (s.getType() == StepType.XML_TO_VERBATIM) {
                  status.setNumberRecords(
                      OBJECT_MAPPER
                          .readValue(msg.get(), PipelinesXmlMessage.class)
                          .getTotalRecordCount());
                } // abcd doesn't have count
              } catch (Exception ex) {
                LOG.warn(
                    "Couldn't get the number of records for dataset {} and attempt {}",
                    status.getDatasetKey(),
                    status.getAttempt(),
                    ex);
              }
            });
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
  private Set<PipelineStep> getStepInfo(String crawlId) {
    return Stream.of(StepType.values())
        .map(
            stepType -> {
              PipelineStep step = new PipelineStep().setType(stepType);

              try {
                Optional<LocalDateTime> startDateOpt =
                    getAsDate(crawlId, Fn.START_DATE.apply(stepType.getLabel()));
                Optional<LocalDateTime> endDateOpt =
                    getAsDate(crawlId, Fn.END_DATE.apply(stepType.getLabel()));
                Optional<Boolean> isErrorOpt =
                    getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(stepType.getLabel()));
                Optional<String> errorMessageOpt =
                    getAsString(crawlId, Fn.ERROR_MESSAGE.apply(stepType.getLabel()));
                Optional<Boolean> isSuccessful =
                    getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(stepType.getLabel()));
                Optional<String> successfulMessageOpt =
                    getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(stepType.getLabel()));

                getAsString(crawlId, Fn.RUNNER.apply(stepType.getLabel()))
                    .ifPresent(r -> step.setRunner(StepRunner.valueOf(r)));

                // dates
                step.setStarted(startDateOpt.orElse(endDateOpt.orElse(null)));
                endDateOpt.ifPresent(step::setFinished);

                if (isErrorOpt.isPresent()) {
                  step.setState(Status.FAILED);
                  errorMessageOpt.ifPresent(step::setMessage);
                } else if (isSuccessful.isPresent()) {
                  step.setState(Status.COMPLETED);
                  successfulMessageOpt.ifPresent(step::setMessage);
                } else if (startDateOpt.isPresent() && !endDateOpt.isPresent()) {
                  step.setState(Status.RUNNING);
                }

                // Gets metrics info fro ELK
                getMetricInfo(crawlId, step).forEach(step::addMetricInfo);

              } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
              }
              return step;
            })
        .collect(Collectors.toSet());
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
