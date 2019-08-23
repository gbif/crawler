package org.gbif.crawler.pipelines;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.status.service.persistence.PipelinesProcessMapper;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep.Status;

import java.text.DecimalFormat;
import java.time.LocalDate;
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
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/** Pipelines monitoring service collects all necessary information from Zookeeper */
public class PipelinesProcessServiceImpl implements PipelinesProcessService {

  private static final Logger LOG = LoggerFactory.getLogger(PipelinesProcessServiceImpl.class);

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
          .build(
              new CacheLoader<String, PipelinesProcessStatus>() {
                @Override
                public PipelinesProcessStatus load(String key) {
                  return null;
                  //              return loadRunningPipelinesProcess(key);
                }
              });

  private final PipelinesProcessMapper pipelinesProcessMapper;

  /**
   * Creates a CrawlerMetricsService. Responsible for interacting with a ZooKeeper instance in a
   * read-only fashion.
   *
   * @param curator to access ZooKeeper
   * @param executor to run the thread pool
   */
  @Inject
  public PipelinesProcessServiceImpl(
      CuratorFramework curator,
      Executor executor,
      RestHighLevelClient client,
      DatasetService datasetService,
      MessagePublisher publisher,
      @Named("pipelines.envPrefix") String envPrefix,
      PipelinesProcessMapper pipelinesProcessMapper) {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.executor = checkNotNull(executor, "executor can't be null");
    this.datasetService = datasetService;
    this.client = client;
    this.envPrefix = envPrefix;
    this.publisher = publisher;
    this.pipelinesProcessMapper = pipelinesProcessMapper;
  }

  /** Reads all monitoring information from Zookeeper pipelines root path */
  @Override
  public Set<PipelinesProcessStatus> getRunningPipelinesProcesses() {
    //    Set<PipelinesProcessStatus> set = new
    // TreeSet<>(Comparator.comparing(PipelinesProcessStatus::getCrawlId));
    Set<PipelinesProcessStatus> set = new HashSet<>();
    try {

      String path = CrawlerNodePaths.buildPath(PipelinesNodePaths.PIPELINES_ROOT);

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
                            PipelinesProcessStatus process = getRunningPipelinesProcess(id);
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
  public PipelinesProcessStatus getRunningPipelinesProcess(String crawlId) {
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
  public void deleteRunningPipelinesProcess(String crawlId) {
    try {
      String path = PipelinesNodePaths.getPipelinesInfoPath(crawlId);
      if (checkExists(path)) {
        curator.delete().deletingChildrenIfNeeded().forPath(path);
      }
    } catch (Exception ex) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", ex);
    }
  }

  /** Removes pipelines root Zookeeper path */
  @Override
  public void deleteAllRunningPipelinesProcess() {
    deleteRunningPipelinesProcess(null);
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
        deleteRunningPipelinesProcess(crawlId + "/" + stepName);
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
    return PipelinesNodePaths.ALL_STEPS;
  }

  @Override
  public Set<PipelinesProcessStatus> getPipelinesProcessesByDatasetKey(String datasetKey) {
    //    Set<PipelinesProcessStatus> set = new
    // TreeSet<>(Comparator.comparing(PipelinesProcessStatus::getCrawlId));
    Set<PipelinesProcessStatus> set = new HashSet<>();
    try {

      String path = CrawlerNodePaths.buildPath(PipelinesNodePaths.PIPELINES_ROOT);

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
                            PipelinesProcessStatus process = getRunningPipelinesProcess(id);
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

  /** Gets step info from ZK */
  private Set<PipelinesStep> getStepInfo(String crawlId) {
    return PipelinesNodePaths.ALL_STEPS.stream()
        .map(
            path -> {
              //      PipelinesStep step = new PipelinesStep(path);
              PipelinesStep step = new PipelinesStep();

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
                //        startDateOpt.ifPresent(x -> step.setStarted(x.toString()));
                //        endDateOpt.ifPresent(x -> step.setFinished(x.toString()));

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
    String infoPath = PipelinesNodePaths.getPipelinesInfoPath(crawlId, path);
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
