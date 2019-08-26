package org.gbif.crawler.status.service.impl;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.crawler.status.service.PipelinesCoordinatorService;
import org.gbif.crawler.status.service.model.PipelinesProcessStatus;
import org.gbif.crawler.status.service.model.PipelinesStep;
import org.gbif.crawler.status.service.persistence.PipelinesProcessMapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
  public void runLastAttempt(UUID datasetKey, Set<PipelinesStep.StepName> steps) {
    Integer lastAttempt = 0; //Get the last successful attempt of each step
    runPipelineAttempt(datasetKey, lastAttempt, steps);
  }

  private Optional<PipelinesStep> getLatestSuccessfulStep(PipelinesProcessStatus pipelinesProcessStatus, PipelinesStep.StepName step) {
    return  pipelinesProcessStatus.getSteps().stream()
              .filter(s -> step.equals(s.getName()))
              .max(Comparator.comparing(PipelinesStep::getStarted));
  }

  @Override
  public void runPipelineAttempt(UUID datasetKey, Integer attempt,
                                 Set<PipelinesStep.StepName> steps) {
    Preconditions.checkNotNull(datasetKey, "Dataset can't be null");
    Preconditions.checkNotNull(attempt, "Attempt can't be null");
    Preconditions.checkNotNull(steps, "Steps can't be null");

    PipelinesProcessStatus status = mapper.get(datasetKey, attempt);
    steps.forEach(stepName ->
        getLatestSuccessfulStep(status, stepName).ifPresent(step -> {
          try {
            if (stepName == PipelinesStep.StepName.HIVE_VIEW || stepName == PipelinesStep.StepName.INTERPRETED_TO_INDEX) {
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesInterpretedMessage.class));
            } else if (steps.contains(PipelinesStep.StepName.VERBATIM_TO_INTERPRETED)) {
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesVerbatimMessage.class));
            } else if (steps.contains(PipelinesStep.StepName.DWCA_TO_VERBATIM)) {
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesDwcaMessage.class));
            } else if (steps.contains(PipelinesStep.StepName.ABCD_TO_VERBATIM)) {
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesAbcdMessage.class));
            } else if (steps.contains(PipelinesStep.StepName.XML_TO_VERBATIM)) {
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesXmlMessage.class));
            } else {
              throw new IllegalArgumentException("Step [" + stepName.name() + "] not supported by this service");
            }
          } catch (IOException ex) {
            LOG.error("Error reading message", ex);
            throw Throwables.propagate(ex);
          }
       })
    );
  }
}
