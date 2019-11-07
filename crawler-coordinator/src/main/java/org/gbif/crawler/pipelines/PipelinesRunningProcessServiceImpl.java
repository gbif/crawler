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
import org.gbif.crawler.constants.PipelinesNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.pipelines.search.PipelinesRunningProcessSearchService;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.model.pipelines.PipelineProcess.STEPS_COMPARATOR;
import static org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import static org.gbif.api.model.pipelines.PipelineStep.Status;
import static org.gbif.crawler.constants.PipelinesNodePaths.DELIMITER;
import static org.gbif.crawler.constants.PipelinesNodePaths.PIPELINES_ROOT;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.INITIALIZED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_ADDED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_REMOVED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_UPDATED;

/** Pipelines monitoring service collects all necessary information from Zookeeper */
public class PipelinesRunningProcessServiceImpl implements PipelinesRunningProcessService {

  private static final Logger LOG = LoggerFactory.getLogger(PipelinesRunningProcessServiceImpl.class);

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
  private static final String FILTER_NAME = "org.gbif.pipelines.transforms.";
  private static final DecimalFormat DF = new DecimalFormat("0");
  private static final BiFunction<UUID, Integer, String> CRAWL_ID_GENERATOR =
    (datasetKey, attempt) -> datasetKey + "_" + attempt;

  private final CuratorFramework curator;
  private final ExecutorService executorService;
  private final RestHighLevelClient client;
  private final String envPrefix;
  private final DatasetService datasetService;
  private final Cache<String, PipelineProcess> processCache =
      Cache2kBuilder.of(String.class, PipelineProcess.class)
          .entryCapacity(Long.MAX_VALUE)
          .suppressExceptions(false)
          .eternal(true)
          .build();
  private final PipelinesRunningProcessSearchService searchService;

  /**
   * Creates a CrawlerMetricsService. Responsible for interacting with a ZooKeeper instance in a
   * read-only fashion.
   *
   * @param curator to access ZooKeeper
   */
  @Inject
  public PipelinesRunningProcessServiceImpl(
      CuratorFramework curator,
      ExecutorService executorService,
      RestHighLevelClient client,
      DatasetService datasetService,
      @Named("pipelines.envPrefix") String envPrefix) throws Exception {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.executorService = executorService;
    this.client = client;
    this.datasetService = datasetService;
    this.envPrefix = envPrefix;
    this.searchService = new PipelinesRunningProcessSearchService(Files.createTempDir().getPath());
    setupTreeCache();
  }

  private void setupTreeCache() throws Exception {
    TreeCache cache = new TreeCache(curator, CrawlerNodePaths.buildPath(PIPELINES_ROOT));
    cache.start();

    Function<String, Optional<String>> relativePath =
        path -> {
          String[] paths = path.substring(path.indexOf(PIPELINES_ROOT)).split(DELIMITER);
          if (paths.length > 1) {
            return Optional.of(paths[1]);
          }
          return Optional.empty();
        };

    TreeCacheListener listener =
        (curatorClient, event) -> {
          // we only add in the cache the events that update the start or end date of a step
          if ((event.getType() == NODE_ADDED || event.getType() == NODE_UPDATED)
              && (event.getData().getPath().contains(PipelinesNodePaths.START)
                  || event.getData().getPath().contains(PipelinesNodePaths.END))) {
            relativePath
                .apply(event.getData().getPath())
                .ifPresent(
                    path ->
                        loadRunningPipelineProcess(path)
                            .ifPresent(process -> {
                              processCache.put(path, process);
                              searchService.index(process);
                            }));
          } else if (event.getType() == NODE_REMOVED) {
            relativePath.apply(event.getData().getPath()).ifPresent(nodePath -> {
              searchService.delete(processCache.get(nodePath));
              processCache.remove(nodePath);

            });
          } else if (event.getType() == INITIALIZED) {
            LOG.info("ZK TreeCache initialized for pipelines");
          }
        };
    cache.getListenable().addListener(listener, executorService);

    Runtime.getRuntime().addShutdownHook(new Thread(cache::close));
  }


  @Override
  public Set<PipelineProcess> getPipelineProcesses() {
    return getPipelineProcesses(null);
  }

  @Override
  public Set<PipelineProcess> getPipelineProcesses(@Nullable UUID datasetKey) {
    return StreamSupport.stream(processCache.entries().spliterator(), true)
        .filter(node -> datasetKey == null || node.getKey().startsWith(datasetKey.toString()))
        .map(CacheEntry::getValue)
        .collect(
            Collectors.toCollection(
                () ->
                    new TreeSet<>(
                        Comparator.comparing(PipelineProcess::getDatasetKey)
                            .thenComparing(PipelineProcess::getAttempt))));
  }

  @Override
  public List<PipelineProcess> searchByDatasetTitle(String datasetTitleQ, int pageNumber, int pageSize) {
    return searchService.searchByDatasetTitle(datasetTitleQ, pageNumber, pageSize, processCache);
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
      // Here we're trying to load all information from Zookeeper into the DatasetProcessStatus
      // object
      PipelineProcess process =
          new PipelineProcess()
              .setDatasetKey(UUID.fromString(ids[0]))
              .setAttempt(Integer.parseInt(ids[1]));

      // ALL_STEPS - static set of all pipelines steps: DWCA_TO_AVRO, VERBATIM_TO_INTERPRETED and etc.
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
        .max(STEPS_COMPARATOR)
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

  /**
   * Gets metrics info from ELK
   */
  private Set<MetricInfo> getMetricInfo(String crawlId, PipelineStep step) {
    if (client != null) {
      try {
        SearchResponse search = client.search(getEsMetricQuery(crawlId, step));

        Aggregations aggregations = search.getAggregations();
        if (aggregations == null) {
          return Collections.emptySet();
        }

        ParsedStringTerms uniqueName = aggregations.get("unique_name");
        if (uniqueName == null) {
          return Collections.emptySet();
        }

        return uniqueName.getBuckets().stream().map(x -> {
          String keyAsString = x.getKeyAsString();
          String keyFormatted = keyAsString.substring(keyAsString.indexOf(FILTER_NAME) + FILTER_NAME.length());

          ParsedMax value = x.getAggregations().get("max_value");
          double valueDouble = value.getValue();
          String valueFormatted = DF.format(valueDouble);

          return new MetricInfo(keyFormatted, valueFormatted);
        }).collect(Collectors.toSet());

      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
      }
    }
    return Collections.emptySet();
  }

  /**
   * ES query like:
   *
   * <pre>{@code
   * {
   *   "size": 0,
   *   "aggs": {
   *     "unique_name": {
   *       "terms": {"field": "name.keyword"},
   *       "aggs": {
   *         "max_value": {
   *           "max": {"field": "value"}
   *         }
   *       }
   *     }
   *   },
   *   "query": {
   *     "bool": {
   *       "must": [
   *         {"match": {"datasetId": "7a25f7aa-03fb-4322-aaeb-66719e1a9527"}},
   *         {"match": {"attempt": "52"}},
   *         {"match": {"type": "GAUGE"}},
   *         {"match_phrase_prefix": { "name": "driver.PipelinesOptionsFactory"}}
   *       ]
   *     }
   *   }
   * }
   *
   * }</pre>
   */
  private SearchRequest getEsMetricQuery(String crawlId, PipelineStep step) {

    String[] ids = crawlId.split("_");
    String year = String.valueOf(LocalDate.now().getYear());

    // ES query
    SearchRequest searchRequest = new SearchRequest(envPrefix + "-pipeline-metric-" + year + ".*");
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    // Must
    BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
    booleanQuery.must(QueryBuilders.matchQuery("datasetId", ids[0]));
    booleanQuery.must(QueryBuilders.matchQuery("attempt", ids[1]));
    booleanQuery.must(QueryBuilders.matchQuery("step", step.getType().name()));
    booleanQuery.must(QueryBuilders.rangeQuery("@timestamp").gte(step.getStarted()));
    booleanQuery.must(QueryBuilders.matchQuery("type", "GAUGE"));
    booleanQuery.must(
        QueryBuilders.matchPhrasePrefixQuery("name", "driver.PipelinesOptionsFactory"));
    searchSourceBuilder.query(booleanQuery);

    // Aggr
    searchSourceBuilder.aggregation(
        AggregationBuilders.terms("unique_name")
            .field("name.keyword")
            .subAggregation(AggregationBuilders.max("max_value").field("value")));

    searchSourceBuilder.size(0);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }
}
