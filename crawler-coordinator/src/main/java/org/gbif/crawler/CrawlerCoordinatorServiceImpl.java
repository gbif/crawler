/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.Constants;
import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.util.MachineTagUtils;
import org.gbif.api.util.comparators.EndpointCreatedComparator;
import org.gbif.api.util.comparators.EndpointPriorityComparator;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.metasync.api.MetadataSynchronizer;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.gbif.api.vocabulary.TagName.CRAWL_ATTEMPT;
import static org.gbif.api.vocabulary.TagName.DECLARED_COUNT;
import static org.gbif.crawler.constants.CrawlerNodePaths.ABCDA_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.CAMETRAPDP_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_INFO;
import static org.gbif.crawler.constants.CrawlerNodePaths.DWCA_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.QUEUED_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.RUNNING_CRAWLS;
import static org.gbif.crawler.constants.CrawlerNodePaths.XML_CRAWL;
import static org.gbif.crawler.constants.CrawlerNodePaths.buildPath;

/**
 * This implementation stores crawls in ZooKeeper in a node structure like this:
 *
 * <ul>
 *   <li>{@code /crawls/<uuid>} holds general information about this crawl. The data attached to
 *       this node is supposed to be immutable throughout the crawl but subnodes will hold
 *       information about the current state of the job.
 *   <li>{@code /queuedCrawls/<queuename>} is used as a priority queue. The children can be ordered
 *       lexicographically to get the order in which crawl jobs are supposed to be picked up
 *   <li>{@code /runningCrawls/<queuename>} is used as a lock for {@code /queuedCrawls}. Every
 *       running crawler creates a lock here as long as it's running so that in case of failure the
 *       job can be picked up
 * </ul>
 *
 * <p>This uses the Apache Curator framework to help implement the Queue in ZooKeeper. For more
 * information see the <a
 * href="https://curator.apache.org/curator-recipes/distributed-priority-queue.html">documentation</a>.
 * Please note that this means that we rely on Curator's serialization format for the
 * <em>queuedCrawls</em> and <em>runningCrawls</em> nodes.
 */
public class CrawlerCoordinatorServiceImpl implements CrawlerCoordinatorService {

  public static final String METADATA_NAMESPACE = "metasync.gbif.org";
  private static final Logger LOG = LoggerFactory.getLogger(CrawlerCoordinatorServiceImpl.class);
  private static final Comparator<Endpoint> ENDPOINT_COMPARATOR =
      Ordering.compound(
          Lists.newArrayList(
              Collections.reverseOrder(new EndpointPriorityComparator()),
              EndpointCreatedComparator.INSTANCE));
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int DEFAULT_PRIORITY = 0;
  private final CuratorFramework curator;
  private final DistributedPriorityQueue<UUID> xmlQueue;
  private final DistributedPriorityQueue<UUID> dwcaQueue;
  private final DistributedPriorityQueue<UUID> abcdaQueue;
  private final DistributedPriorityQueue<UUID> camtrapDpQueue;
  private final DatasetService datasetService;
  private final MetadataSynchronizer metadataSynchronizer;
  private final CrawlerCoordinatorServiceMetrics metrics = new CrawlerCoordinatorServiceMetrics();

  /**
   * Creates a CrawlerCoordinatorService for a specific ZooKeeper instance, pointing to a remote
   * Registry WS.
   *
   * @param curator to access ZooKeeper
   * @param datasetService to access the Registry
   * @throws ServiceUnavailableException when there was a failure during initialization of ZooKeeper
   */
  public CrawlerCoordinatorServiceImpl(
      CuratorFramework curator,
      DatasetService datasetService,
      MetadataSynchronizer metadataSynchronizer) {
    this.curator = checkNotNull(curator, "curator can't be null");
    this.datasetService = checkNotNull(datasetService, "datasetService can't be null");
    this.metadataSynchronizer =
        checkNotNull(metadataSynchronizer, "metadataSynchronizer can't be null");

    xmlQueue = buildQueue(curator, XML_CRAWL);
    dwcaQueue = buildQueue(curator, DWCA_CRAWL);
    abcdaQueue = buildQueue(curator, ABCDA_CRAWL);
    camtrapDpQueue = buildQueue(curator, CAMETRAPDP_CRAWL);
  }

  @Override
  public void initiateCrawl(UUID datasetKey, int priority, Platform platform) {
    MDC.put("datasetKey", datasetKey.toString());

    metrics.timerStart();
    try {
      doScheduleCrawl(datasetKey, priority, platform);
    } catch (RuntimeException e) {
      metrics.unsuccessfulSchedule();
      throw e;
    } finally {
      metrics.timerStop();
    }

    metrics.successfulSchedule(datasetKey);
    MDC.remove("datasetKey");
    MDC.remove("attempt");
  }

  @Override
  public void initiateCrawl(UUID datasetKey, Platform platform) {
    initiateCrawl(datasetKey, DEFAULT_PRIORITY, platform);
  }

  /**
   * Returns a list of valid Endpoints for the currently available Crawler implementations in a
   * priority sorted order.
   *
   * @param endpoints to sort
   * @return sorted and filtered list of Endpoints with the <em>best</em> ones at the beginning of
   *     the list
   * @see EndpointPriorityComparator
   */
  @VisibleForTesting
  List<Endpoint> prioritySortEndpoints(List<Endpoint> endpoints) {
    checkNotNull(endpoints, "endpoints can't be null");

    // Filter out all Endpoints that we can't crawl
    List<Endpoint> result = Lists.newArrayList();
    for (Endpoint endpoint : endpoints) {
      if (EndpointPriorityComparator.PRIORITIES.contains(endpoint.getType())) {
        result.add(endpoint);
      }
    }

    // Sort the remaining ones
    result.sort(ENDPOINT_COMPARATOR);
    return result;
  }

  /**
   * Builds a Curator queue for the given path. The path will be a subpath to {@link
   * CrawlerNodePaths#QUEUED_CRAWLS}.
   */
  private DistributedPriorityQueue<UUID> buildQueue(CuratorFramework curator, String path) {
    QueueBuilder<UUID> builder =
        QueueBuilder.builder(curator, null, new UuidSerializer(), buildPath(path, QUEUED_CRAWLS));
    DistributedPriorityQueue<UUID> queue =
        builder.lockPath(buildPath(path, RUNNING_CRAWLS)).buildPriorityQueue(1);

    try {
      curator
          .create()
          .orSetData()
          .creatingParentContainersIfNeeded()
          .forPath(buildPath(CRAWL_INFO));
      queue.start();
    } catch (Exception e) {
      throw new ServiceUnavailableException("Error starting up Priority queue", e);
    }

    return queue;
  }

  /**
   * This is the main entry point into this service and should be called by all methods wanting to
   * schedule a crawl.
   */
  private void doScheduleCrawl(UUID datasetKey, int priority, Platform platform) {
    checkNotNull(datasetKey, "datasetKey can't be null");

    // We first have to check if the dataset can be crawled.
    Dataset dataset = checkDataset(datasetService.get(datasetKey), datasetKey);
    Optional<Endpoint> endpoint = getEndpointToCrawl(dataset);
    if (!endpoint.isPresent()) {
      metrics.noValidEndpoint();
      throw new IllegalArgumentException("No eligible endpoints for dataset [" + datasetKey + "]");
    }

    // Ask the source how many records it has
    Long declaredCount = updateOrRetrieveDeclaredRecordCount(dataset, endpoint.get(), datasetKey);

    // This object holds all information needed by Crawlers to crawl the endpoint
    CrawlJob crawlJob = getCrawlJob(datasetKey, dataset, endpoint.get(), declaredCount, platform);
    MDC.put("attempt", String.valueOf(crawlJob.getAttempt()));

    byte[] dataBytes = serializeCrawlJob(crawlJob);
    queueCrawlJob(
        datasetKey,
        isAbcdArchive(endpoint.get()),
        isDarwinCoreArchive(endpoint.get()),
        isCamtrapDpArchive(endpoint.get()),
        priority,
        dataBytes);
    LOG.info("Crawling endpoint [{}] for dataset [{}]", endpoint.get().getUrl(), datasetKey);
    writeDeclaredRecordCount(datasetKey, declaredCount);
  }

  /** Query the source for an expected count of records. */
  private Long updateOrRetrieveDeclaredRecordCount(
      Dataset dataset, Endpoint endpoint, UUID datasetKey) {
    Long declaredCount = null;

    List<MachineTag> filteredTags = MachineTagUtils.list(dataset, DECLARED_COUNT);

    if (filteredTags.size() > 1) {
      LOG.warn("Found more than one declaredCount for dataset [{}]. Ignoring.", dataset.getKey());
    } else if (filteredTags.size() == 1) {
      declaredCount = Long.parseLong(filteredTags.get(0).getValue());
      LOG.debug("Existing declaredCount is {}", declaredCount);
    }

    try {
      LOG.debug("Attempting update of declared count");
      Long newDeclaredCount = metadataSynchronizer.getDatasetCount(dataset, endpoint);

      if (newDeclaredCount != null) {
        datasetService.deleteMachineTags(datasetKey, DECLARED_COUNT);
        datasetService.addMachineTag(
            dataset.getKey(), DECLARED_COUNT, Long.toString(newDeclaredCount));
        LOG.debug("New declared count is {}", newDeclaredCount);
        declaredCount = newDeclaredCount;
      } else {
        LOG.debug("No new declared count");
      }
    } catch (Exception e) {
      LOG.error("Error updating declared count " + e.getMessage(), e);
    }

    return declaredCount;
  }

  /**
   * Determine if an endpoint should be handled as a DarwinCore archive or not. Note: EML are
   * handled as DarwinCore archive with no data (aka metadata only).
   *
   * @param endpoint
   * @return
   */
  private static boolean isDarwinCoreArchive(Endpoint endpoint) {
    return EndpointType.DWC_ARCHIVE == endpoint.getType() || EndpointType.EML == endpoint.getType();
  }

  /**
   * Determine if an endpoint should be handled as an ABCD archive or not.
   *
   * @param endpoint
   * @return
   */
  private static boolean isAbcdArchive(Endpoint endpoint) {
    return EndpointType.BIOCASE_XML_ARCHIVE == endpoint.getType();
  }

  /**
   * Determine if an endpoint should be handled as an CamtrapDP archive or not.
   *
   * @param endpoint
   * @return
   */
  private static boolean isCamtrapDpArchive(Endpoint endpoint) {
    return EndpointType.CAMTRAP_DP_v_0_4 == endpoint.getType();
  }

  /**
   * Some of our datasets declare how many records they hold. We save this count in ZooKeeper to
   * later display it on the admin console.
   */
  private void writeDeclaredRecordCount(UUID datasetKey, Long declaredCount) {
    if (declaredCount != null) {
      try {
        curator
            .create()
            .creatingParentsIfNeeded()
            .forPath(
                CrawlerNodePaths.getCrawlInfoPath(datasetKey, CrawlerNodePaths.DECLARED_COUNT),
                declaredCount.toString().getBytes(Charsets.UTF_8));
      } catch (Exception e) {
        throw new ServiceUnavailableException("Error communicating with ZooKeeper", e);
      }
    }
  }

  /**
   * Checks if a dataset is eligible to be crawled throwing {@link IllegalArgumentException} if not.
   *
   * <p>
   *
   * <ul>
   *   <li>It needs to exist
   *   <li>It mustn't be scheduled already
   *   <li>It must have endpoints
   * </ul>
   *
   * <p>This method does not check the validity of the endpoints itself. And this check is dependent
   * on external state so the output might change between calls.
   *
   * @param dataset to check
   * @param datasetKey of the dataset to check, passed in separately to generate error messages in
   *     case the dataset doesn't even exist
   * @return the dataset
   * @throws RuntimeException when there is a problem communicating with ZooKeeper
   */
  private Dataset checkDataset(Dataset dataset, UUID datasetKey) {
    // Does the dataset exist?
    if (dataset == null) {
      throw new IllegalArgumentException("Dataset [" + datasetKey + "] does not exist");
    }

    if (dataset.getDeleted() != null) {
      throw new IllegalArgumentException("Dataset [" + datasetKey + "] is deleted");
    }

    if (Constants.NUB_DATASET_KEY.equals(dataset.getKey())) {
      throw new IllegalArgumentException("Backbone dataset [" + datasetKey + "] cannot be indexed");
    }

    // Is the dataset already scheduled to be crawled or currently being crawled?
    Stat crawlNode;
    try {
      crawlNode = curator.checkExists().forPath(CrawlerNodePaths.getCrawlInfoPath(datasetKey));
    } catch (Exception e) {
      throw new ServiceUnavailableException("Exception while verifying dataset", e);
    }
    if (crawlNode != null) {
      metrics.alreadyScheduled();
      throw new AlreadyCrawlingException(
          "Requested crawl for dataset ["
              + datasetKey
              + "] but crawl already scheduled or running, ignoring");
    }

    // Does the dataset have any endpoints?
    if (dataset.getEndpoints().isEmpty()) {
      metrics.noValidEndpoint();
      throw new IllegalArgumentException(
          "Dataset [" + datasetKey + "] does not have any endpoints");
    }

    return dataset;
  }

  /**
   * Gets the endpoint that we want to crawl from the passed in dataset.
   *
   * <p>We take into account a list of supported and prioritized endpoint types and verify that the
   * declared dataset type matches a supported endpoint type.
   *
   * @param dataset to get the endpoint for
   * @return will be present if we found an eligible endpoint
   * @see EndpointPriorityComparator
   */
  private Optional<Endpoint> getEndpointToCrawl(Dataset dataset) {
    // Are any of the endpoints eligible to be crawled
    List<Endpoint> sortedEndpoints = prioritySortEndpoints(dataset.getEndpoints());
    if (sortedEndpoints.isEmpty()) {
      return java.util.Optional.empty();
    }
    Endpoint ep = sortedEndpoints.get(0);
    return java.util.Optional.ofNullable(ep);
  }

  /**
   * Populates a CrawlJob object with various protocol specific properties (using key-value pairs).
   *
   * <p>These key-value pairs are using magic strings so whatever reads these things from ZooKeeper
   * must use the same terminology. This was done to avoid polymorphic deserialization.
   *
   * @param datasetKey of the dataset to crawl,
   * @param dataset to crawl
   * @param endpoint to crawl
   * @return populated object ready to be put into ZooKeeper
   */
  private CrawlJob getCrawlJob(
      UUID datasetKey, Dataset dataset, Endpoint endpoint, Long declaredCount, Platform platform) {
    checkNotNull(dataset, "dataset can't be null");
    checkNotNull(endpoint, "endpoint can't be null");

    Map<String, String> properties = Maps.newHashMap();
    switch (endpoint.getType()) {
      case DIGIR:
        fillPropertyFromTags(
            datasetKey, dataset.getMachineTags(), properties, METADATA_NAMESPACE, "code", true);
        properties.put("manis", "false");
        break;
      case DIGIR_MANIS:
        fillPropertyFromTags(
            datasetKey, dataset.getMachineTags(), properties, METADATA_NAMESPACE, "code", true);
        properties.put("manis", "true");
        break;
      case TAPIR:
        fillPropertyFromTags(
            datasetKey,
            dataset.getMachineTags(),
            properties,
            METADATA_NAMESPACE,
            "conceptualSchema",
            true);
        break;
      case BIOCASE:
        properties.put("datasetTitle", dataset.getTitle());
        fillPropertyFromTags(
            datasetKey,
            endpoint.getMachineTags(),
            properties,
            METADATA_NAMESPACE,
            "conceptualSchema",
            true);
        break;
      case DWC_ARCHIVE:
      case EML:
        break;
      default:
        // This should never happen as it should've been caught earlier in the process
        // probably a wrongly registered dataset
        LOG.warn(
            "Unsupported endpoint for occurrence dataset [{}] - [{}]",
            datasetKey,
            endpoint.getType());
        break;
    }

    properties.put("platform", platform.name());

    if (declaredCount != null) {
      properties.put("declaredCount", String.valueOf(declaredCount));
    }

    /* NOTE/TODO: This introduces a race-condition. If two coordinators were to do this at the same time they'd get a
     new attempt number but only one of them could later create the node.

     There is no easy solution to this: We could first take out a lock by creating the path for this UUID but then
     we'd need to change the DatasetProcessService to ignore everything that doesn't have content which means an extra
     check for each dataset. Another solution would be to write a dummy attempt number (like 0 or -1)but if a crawler
     then picks up this crawl before we can change it to the proper number the crawler will use the wrong number.
     All other solutions I could come up with were relatively complicated as well.

     I've therefore opted to not change this. That means that the attempt number is not necessarily always increased
     by one but might in very rare cases be incremented by more than one even though only one of these will then be
     used.

     This is definitely a design flaw and should be addressed at some point.
    */
    int attempt = getAttempt(datasetKey, dataset, endpoint);

    return new CrawlJob(datasetKey, endpoint.getType(), endpoint.getUrl(), attempt, properties);
  }

  /**
   * This retrieves the current attempt of crawling for this dataset, increments it by one (in the
   * registry) and returns this new number.
   */
  private int getAttempt(UUID datasetKey, Dataset dataset, Endpoint endpoint) {
    int attempt = 0;

    // Find the maximum last attempt ID if any exist, deleting all others
    for (MachineTag tag : MachineTagUtils.list(dataset, CRAWL_ATTEMPT)) {
      int tagKey = tag.getKey();
      try {
        int taggedAttempt = Integer.parseInt(tag.getValue());
        attempt = Math.max(taggedAttempt, attempt);
      } catch (NumberFormatException e) {
        // Swallow it - the tag is corrupt and should be removed
      }
      datasetService.deleteMachineTag(datasetKey, tagKey);
    }
    // store updated tag
    attempt++;
    MachineTag tag =
        new MachineTag(
            CRAWL_ATTEMPT.getNamespace().getNamespace(),
            CRAWL_ATTEMPT.getName(),
            String.valueOf(attempt));
    datasetService.addMachineTag(datasetKey, tag);

    metrics.registerCrawl(endpoint);
    return attempt;
  }

  /**
   * Looks for a specific predicate in a namespace and populates the properties map with the value
   * if found, throwing an IllegalArgumentException otherwise.
   *
   * @param datasetKey these tags are from, used for error message
   * @param tags to search through
   * @param properties to populate
   * @param namespace to look for in tags
   * @param predicate to look for in tags
   * @param required if true, and IAE is thrown on missing tags, otherwise silently ignored
   */
  private void fillPropertyFromTags(
      UUID datasetKey,
      Iterable<MachineTag> tags,
      Map<String, String> properties,
      String namespace,
      String predicate,
      boolean required) {
    Optional<String> value = findTag(tags, namespace, predicate);
    if (value.isPresent()) {
      properties.put(predicate, value.get());
    } else if (required) {
      throw new IllegalArgumentException(
          "Could not find ["
              + namespace
              + ":"
              + predicate
              + "] tag for dataset ["
              + datasetKey
              + "]");
    }
  }

  /**
   * Can be used to find the value of a certain namespace-name pair for a collection of tags.
   *
   * @param tags to search in
   * @param namespace to look for
   * @param name to look for
   * @return {@code absent} if no such tag could be found, the value otherwise
   */
  private Optional<String> findTag(Iterable<MachineTag> tags, String namespace, String name) {
    for (MachineTag tag : tags) {
      if (namespace.equals(tag.getNamespace()) && name.equals(tag.getName())) {
        // This assumes that all tags have a non-null value
        return Optional.of(tag.getValue());
      }
    }
    return Optional.empty();
  }

  /**
   * Serializes a {@link CrawlJob} into JSON
   *
   * @param crawlJob to serialize
   * @return byte array of the JSON format of the passed in object
   */
  private byte[] serializeCrawlJob(CrawlJob crawlJob) {
    byte[] dataBytes;
    try {
      dataBytes = MAPPER.writeValueAsBytes(crawlJob);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Error serializing the crawl status object", e);
    }
    return dataBytes;
  }

  /**
   * This puts the passed in byte array in our crawl queue as well as in the general crawl info
   * node.
   *
   * @param datasetKey of the dataset to enqueue
   * @param dataBytes to put into ZooKeeper
   */
  private void queueCrawlJob(
      UUID datasetKey, boolean isAbcda, boolean isDwca, boolean isCamtrapDp, int priority, byte[] dataBytes) {
    try {
      // This could in theory fail when two coordinators are running at the same time. They'll have
      // confirmed if this
      // node exists or not earlier but in the meantime another Coordinator might have created it.
      // That means the
      // Exception we throw does not necessarily tell us enough. But this is an edge-case and I feel
      // it's not worth
      // trying to figure out why this failed.
      curator
          .create()
          .creatingParentsIfNeeded()
          .forPath(CrawlerNodePaths.getCrawlInfoPath(datasetKey), dataBytes);
      if (isAbcda) {
        abcdaQueue.put(datasetKey, priority);
      } else if (isDwca) {
        dwcaQueue.put(datasetKey, priority);
      } else if (isCamtrapDp) {
        camtrapDpQueue.put(datasetKey, priority);
      } else {
        xmlQueue.put(datasetKey, priority);
      }
    } catch (Exception e) {
      throw new ServiceUnavailableException("Error communicating with ZooKeeper", e);
    }
  }
}
