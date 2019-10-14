package org.gbif.crawler.pipelines;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.curator.framework.CuratorFramework;
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

import static org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import static org.gbif.api.model.pipelines.PipelineStep.Status;
import static org.gbif.crawler.constants.PipelinesNodePaths.PIPELINES_ROOT;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import static com.google.common.base.Preconditions.checkNotNull;

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
  private final Executor executor;
  private final RestHighLevelClient client;
  private final String envPrefix;
  private final LoadingCache<String, PipelineProcess> statusCache =
      CacheBuilder.newBuilder()
          .expireAfterWrite(2, TimeUnit.MINUTES)
          .build(CacheLoader.from(this::loadRunningPipelineProcess));

  /**
   * Creates a CrawlerMetricsService. Responsible for interacting with a ZooKeeper instance in a
   * read-only fashion.
   *
   * @param curator to access ZooKeeper
   * @param executor to run the thread pool
   */
  @Inject
  public PipelinesRunningProcessServiceImpl(
      CuratorFramework curator,
      Executor executor,
      RestHighLevelClient client,
      @Named("pipelines.envPrefix") String envPrefix) {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.executor = checkNotNull(executor, "executor can't be null");
    this.client = client;
    this.envPrefix = envPrefix;
  }

  @Override
  public Set<PipelineProcess> getPipelineProcesses() {
    return getPipelineProcesses(null);
  }

  @Override
  public Set<PipelineProcess> getPipelineProcesses(@Nullable UUID datasetKey) {
    Set<PipelineProcess> set =
      new TreeSet<>(
        Comparator.comparing(PipelineProcess::getDatasetKey)
          .thenComparing(PipelineProcess::getAttempt));
    try {
      String path = CrawlerNodePaths.buildPath(PIPELINES_ROOT);

      if (!checkExists(path)) {
        return Collections.emptySet();
      }

      // Reads all nodes in async mode
      CompletableFuture[] futures =
        curator.getChildren().forPath(path).stream()
          .filter(node -> datasetKey == null || node.startsWith(datasetKey.toString()))
          .map(
            id ->
              CompletableFuture.runAsync(
                () -> {
                  PipelineProcess process = getPipelineProcessByCrawlId(id);
                  Optional.ofNullable(process).ifPresent(set::add);
                },
                executor))
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

  @Override
  public PipelineProcess getPipelineProcess(UUID datasetKey, int attempt) {
    return getPipelineProcessByCrawlId(CRAWL_ID_GENERATOR.apply(datasetKey, attempt));
  }

  private PipelineProcess getPipelineProcessByCrawlId(String crawlId) {
    try {
      return statusCache.get(crawlId);
    } catch (ExecutionException e) {
      return null;
    }
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
  private PipelineProcess loadRunningPipelineProcess(String crawlId) {
    checkNotNull(crawlId, "crawlId can't be null");

    try {
      String[] ids = crawlId.split("_");

      // Check if dataset is actually being processed right now
      if (!checkExists(getPipelinesInfoPath(crawlId))) {
          return new PipelineProcess().setDatasetKey(UUID.fromString(ids[0])).setAttempt(Integer.parseInt(ids[1]));
      }
      // Here we're trying to load all information from Zookeeper into the DatasetProcessStatus
      // object
      PipelineProcess status =
          new PipelineProcess()
              .setDatasetKey(UUID.fromString(ids[0]))
              .setAttempt(Integer.parseInt(ids[1]));

      // ALL_STEPS - static set of all pipelines steps: DWCA_TO_AVRO, VERBATIM_TO_INTERPRETED and etc.
      getStepInfo(crawlId).stream().filter(s -> Objects.nonNull(s.getStarted())).forEach(status::addStep);
      addNumberRecords(status, crawlId);

      return status;
    } catch (Exception ex) {
      LOG.error("crawlId {}", crawlId, ex.getCause());
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  private void addNumberRecords(PipelineProcess status, String crawlId) {
    // get number of records
    status.getSteps().stream()
        .filter(s -> s.getType().getExecutionOrder() == 1)
        .max(Comparator.comparing(PipelineStep::getStarted))
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

  /** Gets step info from ZK */
  private Set<PipelineStep> getStepInfo(String crawlId) {
    return Stream.of(StepType.values())
        .map(
            stepType -> {
              PipelineStep step = new PipelineStep().setType(stepType);

              try {
                Optional<LocalDateTime> startDateOpt = getAsDate(crawlId, Fn.START_DATE.apply(stepType.getLabel()));
                Optional<LocalDateTime> endDateOpt = getAsDate(crawlId, Fn.END_DATE.apply(stepType.getLabel()));
                Optional<Boolean> isErrorOpt = getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(stepType.getLabel()));
                Optional<String> errorMessageOpt = getAsString(crawlId, Fn.ERROR_MESSAGE.apply(stepType.getLabel()));
                Optional<Boolean> isSuccessful = getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(stepType.getLabel()));
                Optional<String> successfulMessageOpt = getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(stepType.getLabel()));

                getAsString(crawlId, Fn.RUNNER.apply(stepType.getLabel())).ifPresent(r -> step.setRunner(StepRunner.valueOf(r)));

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
        return Optional.of(new String(responseData, Charsets.UTF_8));
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
