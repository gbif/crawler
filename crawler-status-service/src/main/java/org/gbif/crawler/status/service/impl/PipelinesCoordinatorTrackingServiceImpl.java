package org.gbif.crawler.status.service.impl;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.crawler.status.service.PipelinesHistoryTrackingService;
import org.gbif.crawler.status.service.ReRunPipelineResponse;
import org.gbif.crawler.status.service.model.*;
import org.gbif.crawler.status.service.persistence.PipelineProcessMapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
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
public class PipelinesCoordinatorTrackingServiceImpl implements PipelinesHistoryTrackingService {

  private static Logger LOG = LoggerFactory.getLogger(PipelinesCoordinatorTrackingServiceImpl.class);

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
  private final PipelineProcessMapper mapper;


  @Inject
  public PipelinesCoordinatorTrackingServiceImpl(MessagePublisher publisher, PipelineProcessMapper mapper) {
    this.publisher = publisher;
    this.mapper = mapper;
  }

  @Override
  public ReRunPipelineResponse runLastAttempt(UUID datasetKey, Set<StepType> steps, String reason) {
    Integer lastAttempt = 0; //Get the last successful attempt of each step
    return runPipelineAttempt(datasetKey, lastAttempt, steps, reason);
  }

  private Optional<PipelineStep> getLatestSuccessfulStep(PipelineProcess pipelineProcess, StepType step) {
    return  pipelineProcess.getSteps().stream()
              .filter(s -> step.equals(s.getName()))
              .max(Comparator.comparing(PipelineStep::getStarted));
  }

  private PipelineStep.Status getStatus(PipelineProcess pipelineProcess) {
    Set<PipelineStep> latestSteps = new HashSet<>();
    for (StepType stepType : StepType.values()) {
      pipelineProcess.getSteps()
        .stream()
        .filter(s -> stepType == s.getName())
        .max(Comparator.comparing(PipelineStep::getStarted))
        .ifPresent(latestSteps::add);
    }

    List<PipelineStep.Status> statuses = latestSteps.stream()
                                            .map(PipelineStep::getState)
                                            .collect(Collectors.toList());
    if (statuses.size() == 1 ) {
      return statuses.iterator().next();
    } else {
      if (statuses.contains(PipelineStep.Status.FAILED)) {
        return PipelineStep.Status.FAILED;
      } else if(statuses.contains(PipelineStep.Status.RUNNING)) {
        return PipelineStep.Status.RUNNING;
      } else {
        return PipelineStep.Status.COMPLETED;
      }
    }
  }


  @Override
  public PagingResponse<PipelineProcess> history(PagingRequest request) {
    long count = mapper.count(null, null);
    List<PipelineProcess> statuses = mapper.list(null, null, request);
    return new PagingResponse<>(request, count, statuses);
  }

  @Override
  public PagingResponse<PipelineProcess> history(UUID datasetKey, PagingRequest request) {
    long count = mapper.count(datasetKey, null);
    List<PipelineProcess> statuses = mapper.list(datasetKey, null, request);
    return new PagingResponse<>(request, count, statuses);
  }

  @Override
  public ReRunPipelineResponse runPipelineAttempt(UUID datasetKey, Integer attempt, Set<StepType> steps, String reason) {
    Preconditions.checkNotNull(datasetKey, "Dataset can't be null");
    Preconditions.checkNotNull(attempt, "Attempt can't be null");
    Preconditions.checkNotNull(steps, "Steps can't be null");

    PipelineProcess status = mapper.get(datasetKey, attempt);

    if(getStatus(status) == PipelineStep.Status.RUNNING) {
      return new ReRunPipelineResponse.Builder()
              .setResponseStatus(ReRunPipelineResponse.ResponseStatus.PIPELINE_IN_SUBMITTED)
              .setStep(steps)
             .build();
    }

    ReRunPipelineResponse.Builder responseBuilder = new ReRunPipelineResponse.Builder().setStep(steps);
    steps.forEach(stepName ->
        getLatestSuccessfulStep(status, stepName).ifPresent(step -> {
          try {
            if (stepName == StepType.HIVE_VIEW || stepName == StepType.INTERPRETED_TO_INDEX) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesInterpretedMessage.class));
            } else if (steps.contains(StepType.VERBATIM_TO_INTERPRETED)) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesVerbatimMessage.class));
            } else if (steps.contains(StepType.DWCA_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesDwcaMessage.class));
            } else if (steps.contains(StepType.ABCD_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(ReRunPipelineResponse.ResponseStatus.OK);
              publisher.send(MAPPER.readValue(step.getMessage(), PipelinesAbcdMessage.class));
            } else if (steps.contains(StepType.XML_TO_VERBATIM)) {
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

  @Override
  public PipelineProcess get(UUID datasetKey, Integer attempt) {
    return mapper.get(datasetKey, attempt);
  }

  @Override
  public PipelineProcess create(UUID datasetKey, int attempt, String creator) {
    PipelineProcess pipelineProcess = new PipelineProcess();
    LocalDateTime now = LocalDateTime.now();
    pipelineProcess.setAttempt(attempt);
    pipelineProcess.setCreated(now);
    pipelineProcess.setCreatedBy(creator);
    mapper.create(pipelineProcess);
    return pipelineProcess;
  }

  @Override
  public PipelineStep addPipelineAction(long pipelineProcessKey, PipelineStep pipelineStep) {
    mapper.addPipelineStep(pipelineProcessKey, pipelineStep);
    return pipelineStep;
  }

  @Override
  public void updatePipelineAction(long pipelineStepKey, PipelineStep.Status status) {
    if (PipelineStep.Status.FAILED == status || PipelineStep.Status.COMPLETED == status) {

    }
    mapper.updatePipelineStepState(pipelineStepKey, status);
  }

  @Override
  public PipelinesWorkflow getPipelinesWorkflow(UUID datasetKey, Integer attempt) {
    PipelineProcess process = mapper.get(datasetKey, attempt);

    Map<Integer, Map<StepType, List<PipelineStep>>> stepsByOrderAndName =
      process.getSteps().stream()
        .collect(
          Collectors.groupingBy(
            s -> s.getName().getOrder(),
            () -> new TreeMap<>(Comparator.reverseOrder()),
            Collectors.groupingBy(PipelineStep::getName)));

    PipelinesWorkflow workflow = new PipelinesWorkflow();
    workflow.setDatasetKey(process.getDatasetKey());
    workflow.setAttempt(process.getAttempt());

    // workflow steps
    WorkflowStep workflowStep = null;
    workflow.setInitialStep(workflowStep);

    // iterate from last step to first
    List<WorkflowStep> currentSteps = null;
    for (Map.Entry<Integer, Map<StepType, List<PipelineStep>>> entry :
      stepsByOrderAndName.entrySet()) {
      workflowStep = new WorkflowStep();
      workflowStep.setNextSteps(currentSteps);

      currentSteps =
        entry.getValue().entrySet().stream()
          .map(
            v -> {
              WorkflowStep step = new WorkflowStep();
              step.setStepType(v.getKey());
              step.getAllSteps().addAll(v.getValue());
              step.setLastStep(step.getAllSteps().first());

              return step;
            })
          .collect(Collectors.toList());
    }

    return workflow;
  }
}
