package org.gbif.crawler;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.util.MachineTagUtils;
import org.gbif.api.util.comparators.EndpointCreatedComparator;
import org.gbif.api.util.comparators.EndpointPriorityComparator;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.registry.metasync.api.MetadataSynchroniser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.vocabulary.TagName.DECLARED_COUNT;

/**
 * Service that allows to re-run pipeline steps on an specific attempt.
 */
public class PipelinesCoordinatorService {

  // General steps, each step is a microservice
  public enum Steps {
    ALL,
    DWCA_TO_VERBATIM,
    XML_TO_VERBATIM,
    ABCD_TO_VERBATIM,
    VERBATIM_TO_INTERPRETED,
    INTERPRETED_TO_INDEX,
    HIVE_VIEW
  }

  private static Logger LOG = LoggerFactory.getLogger(PipelinesCoordinatorService.class);

  private static final Comparator<Endpoint> ENDPOINT_COMPARATOR = Ordering.compound(Lists.newArrayList(
    Collections.reverseOrder(new EndpointPriorityComparator()),
    EndpointCreatedComparator.INSTANCE
  ));

  private final DatasetService datasetService;
  private final MetadataSynchroniser metadataSynchroniser;
  private final MessagePublisher publisher;


  @Inject
  public PipelinesCoordinatorService(DatasetService datasetService, MetadataSynchroniser metadataSynchroniser, MessagePublisher publisher) {
    this.datasetService = datasetService;
    this.metadataSynchroniser = metadataSynchroniser;
    this.publisher = publisher;
  }


  /**
   * Gets the endpoint that we want to crawl from the passed in dataset.
   * <p/>
   * We take into account a list of supported and prioritized endpoint types and verify that the declared dataset type
   * matches a supported endpoint type.
   *
   * @param dataset to get the endpoint for
   *
   * @return will be present if we found an eligible endpoint
   *
   * @see EndpointPriorityComparator
   */
  private Optional<Endpoint> getEndpointToCrawl(Dataset dataset) {
    // Are any of the endpoints eligible to be crawled
    return dataset.getEndpoints().stream()
            .filter(e -> EndpointPriorityComparator.PRIORITIES.contains(e.getType()))
            .min(ENDPOINT_COMPARATOR);
  }

  /**
   * Query the source for an expected count of records.
   */
  private Long updateOrRetrieveDeclaredRecordCount(Dataset dataset, Endpoint endpoint) {

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
      Long newDeclaredCount = metadataSynchroniser.getDatasetCount(dataset, endpoint);

      if (newDeclaredCount != null) {
        datasetService.deleteMachineTags(dataset.getKey(), DECLARED_COUNT);
        datasetService.addMachineTag(dataset.getKey(), DECLARED_COUNT, Long.toString(newDeclaredCount));
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

  public void reRunLastAttempt(UUID datasetKey, Steps...steps) {

  }

  public void reRunPipelineAttempt(UUID datasetKey, Integer attempt, Steps...steps) {
    Preconditions.checkNotNull(datasetKey, "Dataset can't be null");
    Preconditions.checkNotNull(attempt, "Attempt can't be null");
    Preconditions.checkNotNull(steps, "Steps can't be null");

    Dataset dataset  = datasetService.get(datasetKey);
    Optional<Endpoint> endpoint = getEndpointToCrawl(dataset);

    if (endpoint.isPresent()) {
      try {
        Long recordCount  = updateOrRetrieveDeclaredRecordCount(dataset, endpoint.get());
        Set<String> pipelinesSteps = Arrays.stream(steps).map(Steps::name).collect(Collectors.toSet());
        if (pipelinesSteps.contains(Steps.HIVE_VIEW.name()) || pipelinesSteps.contains(Steps.INTERPRETED_TO_INDEX.name())) {
          publisher.send(buildPipelinesInterpretedMessage(dataset, attempt, recordCount, pipelinesSteps));
        } else if (pipelinesSteps.contains(Steps.VERBATIM_TO_INTERPRETED.name())) {
          publisher.send(buildPipelinesVerbatimMessage(dataset, attempt, recordCount, pipelinesSteps));
        } else {
          throw new IllegalArgumentException("Step [" + String.join(",", pipelinesSteps) + "] not supported by this service");
        }
      } catch (IOException ex) {
        LOG.error("Error sending message", ex);
        throw Throwables.propagate(ex);
      }
    } else {
      throw new IllegalArgumentException("No eligible endpoints for dataset [" + datasetKey + "]");
    }
  }

  /**
   *
   * Creates a {@link PipelinesBalancerMessage} that wraps a {@link PipelinesInterpretedMessage}.
   */
  private PipelinesBalancerMessage buildPipelinesInterpretedMessage(Dataset dataset, Integer attempt, Long recordCount,
                                                                    Set<String> steps) {
    return buildPipelinesBalancerMessage(new PipelinesInterpretedMessage()
                                           .setDatasetUuid(dataset.getKey())
                                           .setAttempt(attempt)
                                           .setNumberOfRecords(recordCount)
                                           .setPipelineSteps(steps),
                                         PipelinesInterpretedMessage.class );
  }


  /**
   *
   * Creates a {@link PipelinesBalancerMessage} that wraps a {@link PipelinesVerbatimMessage}.
   */
  private PipelinesBalancerMessage buildPipelinesVerbatimMessage(Dataset dataset, Integer attempt, Long recordCount,
                                                                 Set<String> steps) {
    return buildPipelinesBalancerMessage(new PipelinesVerbatimMessage()
                                           .setDatasetUuid(dataset.getKey())
                                           .setAttempt(attempt)
                                           .setValidationResult(new PipelinesVerbatimMessage.ValidationResult().setNumberOfRecords(recordCount))
                                           .setPipelineSteps(steps),
                                         PipelinesVerbatimMessage.class);
  }

  /**
   * Creates a {@link PipelinesBalancerMessage} that wraps another message derived from a message class.
   */
  private PipelinesBalancerMessage buildPipelinesBalancerMessage(Message message, Class<?> classs) {
    return new PipelinesBalancerMessage()
            .setMessageClass(classs.getSimpleName())
            .setPayload(message.toString());
  }


  public PipelinesDwcaMessage buildPipelinesDwcaMessage(Dataset dataset, Endpoint endpoint, Integer attempt,
                                                        Long recordCount, Set<String> steps) {
    return new PipelinesDwcaMessage()
            .setAttempt(attempt)
            .setDatasetType(dataset.getType())
            .setDatasetUuid(dataset.getKey())
            .setEndpointType(endpoint.getType())
            .setSource(endpoint.getUrl())
            //.setValidationReport()
            .setPipelineSteps(steps);
  }

  public PipelinesXmlMessage buildPipelinesXmlMessage(Dataset dataset, Endpoint endpoint, Integer attempt,
                                                      Long recordCount, Set<String> steps) {

    PipelinesXmlMessage message = new PipelinesXmlMessage()
                                    .setAttempt(attempt)
                                    .setDatasetUuid(dataset.getKey())
                                    .setEndpointType(endpoint.getType())
                                    //.setReason(FinishReason.UNKNOWN)
                                    .setPipelineSteps(steps);
    Optional.ofNullable(recordCount).ifPresent( val -> message.setTotalRecordCount(val.intValue()));
    return message;
  }

  public PipelinesAbcdMessage buildPipelinesAbcdMessage(Dataset dataset, Endpoint endpoint, Integer attempt,
                                                        Set<String> steps) {

    return new PipelinesAbcdMessage(dataset.getKey(), endpoint.getUrl(), attempt, dataset.getModified(), false,
                                    steps, endpoint.getType());
  }

}
