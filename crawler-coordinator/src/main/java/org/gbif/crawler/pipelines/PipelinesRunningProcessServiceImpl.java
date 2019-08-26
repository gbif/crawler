package org.gbif.crawler.pipelines;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.status.service.model.PipelinesProcessStatus;
import org.gbif.crawler.status.service.model.PipelinesStep;
import org.gbif.crawler.status.service.model.PipelinesStep.MetricInfo;
import org.gbif.crawler.status.service.model.PipelinesStep.Status;
import org.gbif.crawler.status.service.model.StepName;

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
import java.util.stream.Collectors;

import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.curator.framework.CuratorFramework;
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

import static org.gbif.crawler.constants.PipelinesNodePaths.ALL_STEPS;
import static org.gbif.crawler.constants.PipelinesNodePaths.PIPELINES_ROOT;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import static com.google.common.base.Preconditions.checkNotNull;

/** Pipelines monitoring service collects all necessary information from Zookeeper */
public class PipelinesRunningProcessServiceImpl implements PipelinesRunningProcessService {

  private static final Logger LOG = LoggerFactory.getLogger(PipelinesRunningProcessServiceImpl.class);

  private static final String FILTER_NAME = "org.gbif.pipelines.transforms.";
  private static final DecimalFormat DF = new DecimalFormat("0");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final CuratorFramework curator;
  private final Executor executor;
  private final RestHighLevelClient client;
  private final String envPrefix;
  private final DatasetService datasetService;
  private final MessagePublisher publisher;
  private final LoadingCache<String, PipelinesProcessStatus> statusCache =
      CacheBuilder.newBuilder()
          .expireAfterWrite(2, TimeUnit.MINUTES)
          .build(CacheLoader.from(this::loadRunningPipelinesProcess));

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
      DatasetService datasetService,
      MessagePublisher publisher,
      @Named("pipelines.envPrefix") String envPrefix) {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.executor = checkNotNull(executor, "executor can't be null");
    this.datasetService = datasetService;
    this.client = client;
    this.envPrefix = envPrefix;
    this.publisher = publisher;
  }

  /** Reads all monitoring information from Zookeeper pipelines root path */
  @Override
  public Set<PipelinesProcessStatus> getPipelinesProcesses() {
    Set<PipelinesProcessStatus> set =
        new TreeSet<>(
            Comparator.comparing(PipelinesProcessStatus::getDatasetKey)
                .thenComparing(PipelinesProcessStatus::getAttempt));
    try {
      String path = CrawlerNodePaths.buildPath(PIPELINES_ROOT);

      if (!checkExists(path)) {
        return Collections.emptySet();
      }

      // Reads all nodes in async mode
      CompletableFuture[] futures =
          curator.getChildren().forPath(path).stream()
              .map(
                  id ->
                      CompletableFuture.runAsync(
                          () -> {
                            PipelinesProcessStatus process = getPipelinesProcess(id);
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

  /**
   * Reads monitoring information from Zookeeper by crawlId node path
   *
   * @param crawlId path to dataset info
   */
  @Override
  public PipelinesProcessStatus getPipelinesProcess(String crawlId) {
    try {
      return statusCache.get(crawlId);
    } catch (ExecutionException e) {
      return null;
    }
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  @Override
  public void deletePipelinesProcess(String crawlId) {
    try {
      String path = getPipelinesInfoPath(crawlId);
      if (checkExists(path)) {
        curator.delete().deletingChildrenIfNeeded().forPath(path);
      }
    } catch (Exception ex) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  /** Removes pipelines root Zookeeper path */
  @Override
  public void deleteAllPipelinesProcess() {
    deletePipelinesProcess(null);
  }

  /** Restart last failed pipelines step */
  @Override
  public void restartFailedStepByDatasetKey(String crawlId, String stepName) {
    checkNotNull(crawlId, "crawlId can't be null");
    checkNotNull(stepName, "stepName can't be null");

    try {
      Optional<String> mqMessage = getAsString(crawlId, Fn.MQ_MESSAGE.apply(stepName));
      Optional<String> mqClassName = getAsString(crawlId, Fn.MQ_CLASS_NAME.apply(stepName));
      if (publisher != null && mqClassName.isPresent() && mqMessage.isPresent()) {
        deletePipelinesProcess(crawlId + "/" + stepName);
        Message m =
            (PipelineBasedMessage)
                MAPPER.readValue(mqMessage.get(), Class.forName(mqClassName.get()));
        publisher.send(m);
      }
    } catch (Exception ex) {
      LOG.error("crawlId {}", crawlId, ex.getCause());
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  @Override
  public Set<String> getAllStepsNames() {
    return ALL_STEPS;
  }

  @Override
  public Set<PipelinesProcessStatus> getProcessesByDatasetKey(String datasetKey) {
    Set<PipelinesProcessStatus> set =
        new TreeSet<>(
            Comparator.comparing(PipelinesProcessStatus::getDatasetKey)
                .thenComparing(PipelinesProcessStatus::getAttempt));
    try {
      String path = CrawlerNodePaths.buildPath(PIPELINES_ROOT);

      if (!checkExists(path)) {
        return Collections.emptySet();
      }

      // Reads all nodes in async mode
      CompletableFuture[] futures =
          curator.getChildren().forPath(path).stream()
              .filter(node -> node.startsWith(datasetKey))
              .map(
                  id ->
                      CompletableFuture.runAsync(
                          () -> {
                            PipelinesProcessStatus process = getPipelinesProcess(id);
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

  /**
   * Reads monitoring information from Zookeeper by crawlId node path
   *
   * @param crawlId path to dataset info
   */
  private PipelinesProcessStatus loadRunningPipelinesProcess(String crawlId) {
    checkNotNull(crawlId, "crawlId can't be null");

    try {
      String[] ids = crawlId.split("_");

      // Check if dataset is actually being processed right now
      if (!checkExists(getPipelinesInfoPath(crawlId))) {
          return new PipelinesProcessStatus().setDatasetKey(UUID.fromString(ids[0])).setAttempt(Integer.parseInt(ids[1]));
      }
      // Here we're trying to load all information from Zookeeper into the DatasetProcessStatus
      // object
      PipelinesProcessStatus status =
          new PipelinesProcessStatus()
              .setDatasetKey(UUID.fromString(ids[0]))
              .setAttempt(Integer.parseInt(ids[1]));
      Optional.ofNullable(datasetService)
          .ifPresent(s -> status.setDatasetTitle(s.get(UUID.fromString(ids[0])).getTitle()));

      // ALL_STEPS - static set of all pipelines steps: DWCA_TO_AVRO, VERBATIM_TO_INTERPRETED and etc.
      getStepInfo(crawlId).stream().filter(s -> Objects.nonNull(s.getStarted())).forEach(status::addStep);

      return status;
    } catch (Exception ex) {
      LOG.error("crawlId {}", crawlId, ex.getCause());
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  /** Gets step info from ZK */
  private Set<PipelinesStep> getStepInfo(String crawlId) {
    return ALL_STEPS.stream()
        .map(
            path -> {
              // TODO: use same enum
              PipelinesStep step = new PipelinesStep().setName(StepName.valueOf(path));

              try {
                Optional<LocalDateTime> startDateOpt =
                    getAsDate(crawlId, Fn.START_DATE.apply(path));
                Optional<LocalDateTime> endDateOpt = getAsDate(crawlId, Fn.END_DATE.apply(path));
                Optional<Boolean> isErrorOpt =
                    getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(path));
                Optional<String> errorMessageOpt =
                    getAsString(crawlId, Fn.ERROR_MESSAGE.apply(path));
                Optional<Boolean> isSuccessful =
                    getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(path));
                Optional<String> successfulMessageOpt =
                    getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(path));

                getAsString(crawlId, Fn.RUNNER.apply(path)).ifPresent(step::setRunner);

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
  // FIXME: remove if it's not gonna be used in the monitoring UI
  private Set<MetricInfo> getMetricInfo(String crawlId) {
    if (client != null) {
      try {
        SearchResponse search = client.search(getEsMetricQuery(crawlId));

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
  private SearchRequest getEsMetricQuery(String crawlId) {

    String[] ids = crawlId.split("_");
    String year = String.valueOf(LocalDate.now().getYear());

    // ES query
    SearchRequest searchRequest = new SearchRequest(envPrefix + "-pipeline-metric-" + year + ".*");
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    // Must
    BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
    booleanQuery.must(QueryBuilders.matchQuery("datasetId", ids[0]));
    booleanQuery.must(QueryBuilders.matchQuery("attempt", ids[1]));
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
