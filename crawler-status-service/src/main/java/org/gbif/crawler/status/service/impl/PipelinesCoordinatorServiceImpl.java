package org.gbif.crawler.status.service.impl;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.status.service.PipelinesCoordinatorService;
import org.gbif.crawler.status.service.ReRunPipelineResponse;
import org.gbif.crawler.status.service.persistence.PipelinesProcessMapper;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;

import java.io.IOException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Service that allows to re-run pipeline steps on an specific attempt.
 */
public class PipelinesCoordinatorServiceImpl implements PipelinesCoordinatorService {

  private static Logger LOG = LoggerFactory.getLogger(PipelinesCoordinatorServiceImpl.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    // determines whether encountering of unknown properties (ones that do not map to a property, and there is no
    // "any setter" or handler that can handle it) should result in a failure (throwing a JsonMappingException) or not.
    MAPPER.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    // Enforce use of ISO-8601 format dates (http://wiki.fasterxml.com/JacksonFAQDateHandling)
    MAPPER.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  private final MessagePublisher publisher;
  private final PipelinesProcessMapper mapper;


  @Inject
  public PipelinesCoordinatorServiceImpl(MessagePublisher publisher, PipelinesProcessMapper mapper) {
    this.publisher = publisher;
    this.mapper = mapper;
  }

  @Override
  public ReRunPipelineResponse runLastAttempt(UUID datasetKey, Set<PipelinesProcessStatus.PipelinesStep.StepName> steps) {
    Integer lastAttempt = 0; //Get the last successful attempt of each step
    return runPipelineAttempt(datasetKey, lastAttempt, steps);
  }

  private Optional<PipelinesProcessStatus.PipelinesStep> getLatestSuccessfulStep(PipelinesProcessStatus pipelinesProcessStatus, PipelinesProcessStatus.PipelinesStep.StepName step) {
    return  pipelinesProcessStatus.getSteps().stream()
              .filter(s -> step.equals(s.getName()))
              .max(Comparator.comparing(PipelinesProcessStatus.PipelinesStep::getStarted));
  }

  private PipelinesProcessStatus.PipelinesStep.Status getStatus(PipelinesProcessStatus pipelinesProcessStatus) {
    Set<PipelinesProcessStatus.PipelinesStep> latestSteps = new HashSet<>();
    for (PipelinesProcessStatus.PipelinesStep.StepName stepName : PipelinesProcessStatus.PipelinesStep.StepName.values()) {
      pipelinesProcessStatus.getSteps()
        .stream()
        .filter(s -> stepName == s.getName())
        .max(Comparator.comparing(PipelinesProcessStatus.PipelinesStep::getStarted))
        .ifPresent(latestSteps::add);
    }

    List<PipelinesProcessStatus.PipelinesStep.Status> statuses = latestSteps.stream()
                                                                  .map(PipelinesProcessStatus.PipelinesStep::getState)
                                                                  .collect(Collectors.toList());
    if (statuses.size() == 1 ) {
      return statuses.iterator().next();
    } else {
      if (statuses.contains(PipelinesProcessStatus.PipelinesStep.Status.FAILED)) {
        return PipelinesProcessStatus.PipelinesStep.Status.FAILED;
      } else if(statuses.contains(PipelinesProcessStatus.PipelinesStep.Status.RUNNING)) {
        return PipelinesProcessStatus.PipelinesStep.Status.RUNNING;
      } else {
        return PipelinesProcessStatus.PipelinesStep.Status.COMPLETED;
      }
    }
  }

  @Override
  public ReRunPipelineResponse runPipelineAttempt(UUID datasetKey, Integer attempt,
                                                  Set<PipelinesProcessStatus.PipelinesStep.StepName> steps) {
    Preconditions.checkNotNull(datasetKey, "Dataset can't be null");
    Preconditions.checkNotNull(attempt, "Attempt can't be null");
    Preconditions.checkNotNull(steps, "Steps can't be null");

    PipelinesProcessStatus status = mapper.get(datasetKey, attempt);

    if(getStatus(status) == PipelinesProcessStatus.PipelinesStep.Status.RUNNING) {
      return new ReRunPipelineResponse.Builder()
              .setResponseStatus(ReRunPipelineResponse.ResponseStatus.PIPELINE_IN_SUBMITTED)
              .setSteps(steps)
             .build();
    }

    ReRunPipelineResponse.Builder responseBuilder = new ReRunPipelineResponse.Builder().setSteps(steps);
    steps.forEach(stepName ->
        getLatestSuccessfulStep(status, stepName).ifPresent(step -> {
          try {
            if (stepName == PipelinesProcessStatus.PipelinesStep.StepName.HIVE_VIEW || stepName == PipelinesProcessStatus.PipelinesStep.StepName.INTERPRETED_TO_INDEX) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesInterpretedMessage.class));
            } else if (steps.contains(PipelinesProcessStatus.PipelinesStep.StepName.VERBATIM_TO_INTERPRETED)) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesVerbatimMessage.class));
            } else if (steps.contains(PipelinesProcessStatus.PipelinesStep.StepName.DWCA_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesDwcaMessage.class));
            } else if (steps.contains(PipelinesProcessStatus.PipelinesStep.StepName.ABCD_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesAbcdMessage.class));
            } else if (steps.contains(PipelinesProcessStatus.PipelinesStep.StepName.XML_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesXmlMessage.class));
            } else {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.UNSUPPORTED_STEP);
            }
          } catch (IOException ex) {
            LOG.error("Error reading message", ex);
            throw Throwables.propagate(ex);
          }
       })
    );
    return  responseBuilder.build();
  }
}
