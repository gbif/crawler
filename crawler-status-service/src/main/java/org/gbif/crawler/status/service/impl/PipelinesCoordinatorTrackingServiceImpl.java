package org.gbif.crawler.status.service.impl;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.crawler.status.service.PipelinesHistoryTrackingService;
import org.gbif.crawler.status.service.RunPipelineResponse;
import org.gbif.crawler.status.service.model.*;
import org.gbif.crawler.status.service.persistence.PipelineProcessMapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;

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

  //Used to read serialized messages stored in the data base as strings.
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    // determines whether encountering of unknown properties (ones that do not map to a property, and there is no
    // "any setter" or handler that can handle it) should result in a failure (throwing a JsonMappingException) or not.
    OBJECT_MAPPER.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    // Enforce use of ISO-8601 format dates (http://wiki.fasterxml.com/JacksonFAQDateHandling)
    OBJECT_MAPPER.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  //Publisher of messages in RabbitMq
  private final MessagePublisher publisher;

  //MyBatis mapper
  private final PipelineProcessMapper mapper;


  @Inject
  public PipelinesCoordinatorTrackingServiceImpl(MessagePublisher publisher, PipelineProcessMapper mapper) {
    this.publisher = publisher;
    this.mapper = mapper;
  }

  @Override
  public RunPipelineResponse runLastAttempt(UUID datasetKey, Set<StepType> steps, String reason) {
    Integer lastAttempt = 0; //Get the last successful attempt of each step
    return runPipelineAttempt(datasetKey, lastAttempt, steps, reason);
  }

  /**
   * Search the last step executed of a specific StepType.
   * @param pipelineProcess container of steps
   * @param step to be searched
   * @return optionally, the las step found
   */
  private Optional<PipelineStep> getLatestSuccessfulStep(PipelineProcess pipelineProcess, StepType step) {
    return  pipelineProcess.getSteps().stream()
              .filter(s -> step.equals(s.getName()))
              .max(Comparator.comparing(PipelineStep::getStarted));
  }

  /**
   * Calculates the general state of a {@link PipelineProcess}.
   * If one the latest steps of a specific {@link StepType} has a {@link org.gbif.crawler.status.service.model.PipelineStep.Status#FAILED}, the process is considered as FAILED.
   * If all the latest steps of all {@link StepType} have the same {@link org.gbif.crawler.status.service.model.PipelineStep.Status}, that status  used for the {@link PipelineProcess}.
   * If it has step in {@link org.gbif.crawler.status.service.model.PipelineStep.Status#RUNNING} it is decided as the process status, otherwise is {@link org.gbif.crawler.status.service.model.PipelineStep.Status#COMPLETED}
   *
   * @param pipelineProcess that contains all the steps.
   * @return the calculated status of a {@link PipelineProcess}
   */
  private PipelineStep.Status getStatus(PipelineProcess pipelineProcess) {

    //Collects the latest steps per type.
    Set<PipelineStep.Status> statuses = new HashSet<>();
    for (StepType stepType : StepType.values()) {
      pipelineProcess.getSteps()
        .stream()
        .filter(s -> stepType == s.getName())
        .max(Comparator.comparing(PipelineStep::getStarted))
        .ifPresent(step -> statuses.add(step.getState()));
    }

    //Only has one states, it could means that all steps have the same status
    if (statuses.size() == 1 ) {
      return statuses.iterator().next();
    } else {
      //Checks the states by priority
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
    Objects.requireNonNull(datasetKey, "DatasetKey can't be null");

    long count = mapper.count(datasetKey, null);
    List<PipelineProcess> statuses = mapper.list(datasetKey, null, request);
    return new PagingResponse<>(request, count, statuses);
  }

  @Override
  public RunPipelineResponse runPipelineAttempt(UUID datasetKey, Integer attempt, Set<StepType> steps, String reason) {
    Objects.requireNonNull(datasetKey, "DatasetKey can't be null");
    Objects.requireNonNull(attempt, "Attempt can't be null");
    Objects.requireNonNull(steps, "Steps can't be null");
    Objects.requireNonNull(reason, "Reason can't be null");

    PipelineProcess status = mapper.get(datasetKey, attempt);

    //Checks that the pipelines is not in RUNNING state
    if(getStatus(status) == PipelineStep.Status.RUNNING) {
      return new RunPipelineResponse.Builder()
              .setResponseStatus(RunPipelineResponse.ResponseStatus.PIPELINE_IN_SUBMITTED)
              .setStep(steps)
             .build();
    }

    //Performs the messaging and updates the status onces the message has been sent
    RunPipelineResponse.Builder responseBuilder = RunPipelineResponse.builder().setStep(steps);
    steps.forEach(stepName ->
        getLatestSuccessfulStep(status, stepName).ifPresent(step -> {
          try {
            if (stepName == StepType.HIVE_VIEW || stepName == StepType.INTERPRETED_TO_INDEX) {
              responseBuilder.setResponseStatus(RunPipelineResponse.ResponseStatus.OK);
              publisher.send(OBJECT_MAPPER.readValue(step.getMessage(), PipelinesInterpretedMessage.class));
            } else if (steps.contains(StepType.VERBATIM_TO_INTERPRETED)) {
              responseBuilder.setResponseStatus(RunPipelineResponse.ResponseStatus.OK);
              publisher.send(OBJECT_MAPPER.readValue(step.getMessage(), PipelinesVerbatimMessage.class));
            } else if (steps.contains(StepType.DWCA_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(RunPipelineResponse.ResponseStatus.OK);
              publisher.send(OBJECT_MAPPER.readValue(step.getMessage(), PipelinesDwcaMessage.class));
            } else if (steps.contains(StepType.ABCD_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(RunPipelineResponse.ResponseStatus.OK);
              publisher.send(OBJECT_MAPPER.readValue(step.getMessage(), PipelinesAbcdMessage.class));
            } else if (steps.contains(StepType.XML_TO_VERBATIM)) {
              responseBuilder.setResponseStatus(RunPipelineResponse.ResponseStatus.OK);
              publisher.send(OBJECT_MAPPER.readValue(step.getMessage(), PipelinesXmlMessage.class));
            } else {
              responseBuilder.setResponseStatus(RunPipelineResponse.ResponseStatus.UNSUPPORTED_STEP);
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
    Objects.requireNonNull(datasetKey, "DatasetKey can't be null");
    Objects.requireNonNull(attempt, "Attempt can't be null");

    return mapper.get(datasetKey, attempt);
  }

  @Override
  public PipelineProcess create(UUID datasetKey, Integer attempt, String creator) {
    Objects.requireNonNull(datasetKey, "DatasetKey can't be null");
    Objects.requireNonNull(attempt, "Attempt can't be null");
    Objects.requireNonNull(creator, "Creator can't be null");

    PipelineProcess pipelineProcess = new PipelineProcess();
    LocalDateTime now = LocalDateTime.now();
    pipelineProcess.setAttempt(attempt);
    pipelineProcess.setCreated(now);
    pipelineProcess.setCreatedBy(creator);
    mapper.create(pipelineProcess);
    return pipelineProcess;
  }

  @Override
  public PipelineStep addPipelineStep(Long pipelineProcessKey, PipelineStep pipelineStep) {
    Objects.requireNonNull(pipelineProcessKey, "PipelineProcessKey can't be null");
    Objects.requireNonNull(pipelineStep, "PipelineStep can't be null");

    mapper.addPipelineStep(pipelineProcessKey, pipelineStep);
    return pipelineStep;
  }

  @Override
  public void updatePipelineStep(Long pipelineStepKey, PipelineStep.Status status) {
    Objects.requireNonNull(pipelineStepKey, "PipelineStepKey can't be null");
    Objects.requireNonNull(status, "Status can't be null");

    PipelineStep step = mapper.getPipelineStep(pipelineStepKey);
    step.setState(status);
    if (PipelineStep.Status.FAILED == status || PipelineStep.Status.COMPLETED == status) {
      step.setFinished(LocalDateTime.now());
    }
    mapper.updatePipelineStepState(pipelineStepKey, status);
  }

  @Override
  public PipelineWorkflow getPipelineWorkflow(UUID datasetKey, Integer attempt) {
    Objects.requireNonNull(datasetKey, "datasetKey can't be null");
    Objects.requireNonNull(attempt, "attempt can't be null");

    PipelineProcess process = mapper.get(datasetKey, attempt);

    // group the steps by its execution order in the workflow and then by name. This will create something
    // like:
    // 1 -> DWCA_TO_AVRO -> List<PipelineStep>
    // 2 -> VERBATIM_TO_INTERPRETED -> List<PipelineStep>
    // 3 -> INTERPRETED_TO_INDEX -> List<PipelineStep>
    //   -> HIVE_VIEW -> List<PipelineStep>
    //
    // The map is sorted by the step execution order in descending order
    Map<Integer, Map<StepType, List<PipelineStep>>> stepsByOrderAndName =
        process.getSteps().stream()
            .collect(
                Collectors.groupingBy(
                    s -> s.getName().getExecutionOrder(),
                    () ->
                        new TreeMap<Integer, Map<StepType, List<PipelineStep>>>(
                            Comparator.reverseOrder()),
                    Collectors.groupingBy(PipelineStep::getName)));

    // iterate from steps in the last position to the ones in the first one so that we can create
    // the worfklow hierarchy
    List<WorkflowStep> stepsPreviousIteration = null;
    for (Map.Entry<Integer, Map<StepType, List<PipelineStep>>> stepsByOrder :
        stepsByOrderAndName.entrySet()) {

      List<WorkflowStep> currentSteps = new ArrayList<>();
      for (Map.Entry<StepType, List<PipelineStep>> stepsByType :
          stepsByOrder.getValue().entrySet()) {

        // create workflow step
        WorkflowStep step = new WorkflowStep();
        step.setStepType(stepsByType.getKey());
        step.getAllSteps().addAll(stepsByType.getValue());
        step.setLastStep(step.getAllSteps().iterator().next());
        // link this step to its next steps
        step.setNextSteps(stepsPreviousIteration);

        // accumulate all the steps of this StepType
        currentSteps.add(step);
      }

      // the steps of this iteration now become the steps of the previous iteration
      stepsPreviousIteration = currentSteps;
    }

    // create workflow
    PipelineWorkflow workflow = new PipelineWorkflow();
    workflow.setDatasetKey(process.getDatasetKey());
    workflow.setAttempt(process.getAttempt());
    // the last steps created will be the started steps of the workflow
    workflow.setSteps(stepsPreviousIteration);

    return workflow;
  }
}
