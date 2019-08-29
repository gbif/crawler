package org.gbif.crawler.status.service;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.status.service.impl.PipelinesCoordinatorTrackingServiceImpl;
import org.gbif.crawler.status.service.model.*;
import org.gbif.crawler.status.service.persistence.PipelineProcessMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.gbif.crawler.status.service.model.PipelineStep.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PipelinesCoordinatorTrackingServiceTest {

  @Mock private PipelineProcessMapper pipelineProcessMapper;
  @Mock private MessagePublisher messagePublisher;

  private PipelinesHistoryTrackingService trackingService;

  @Before
  public void setup() throws Exception {
    // TODO: inject datasetService
    trackingService =
        new PipelinesCoordinatorTrackingServiceImpl(messagePublisher, pipelineProcessMapper, null, null);
  }

  @Test
  public void getPipelineWorkflowTest() {
    final UUID uuid = UUID.randomUUID();
    final int attempt = 1;

    // mocks
    PipelineProcess mockProcess = createMockProcess(uuid, attempt);
    when(pipelineProcessMapper.get(uuid, attempt)).thenReturn(mockProcess);

    // create workflow
    PipelineWorkflow workflow = trackingService.getPipelineWorkflow(uuid, attempt);

    // assert results
    assertEquals(uuid, workflow.getDatasetKey());
    assertEquals(attempt, workflow.getAttempt());

    // first level of steps
    assertEquals(StepType.ABCD_TO_VERBATIM, workflow.getSteps().get(0).getLastStep().getName());
    assertEquals(1, workflow.getSteps().size());
    assertEquals(2, workflow.getSteps().get(0).getAllSteps().size());
    assertEquals(1, workflow.getSteps().get(0).getNextSteps().size());
    assertEquals(Status.COMPLETED, workflow.getSteps().get(0).getLastStep().getState());

    // second level of steps
    List<WorkflowStep> secondLevelSteps = workflow.getSteps().get(0).getNextSteps();
    assertEquals(StepType.VERBATIM_TO_INTERPRETED, secondLevelSteps.get(0).getLastStep().getName());
    assertEquals(1, secondLevelSteps.get(0).getAllSteps().size());
    assertEquals(2, workflow.getSteps().get(0).getNextSteps().get(0).getNextSteps().size());

    // third level of steps
    List<WorkflowStep> thirdLevelSteps =
        workflow.getSteps().get(0).getNextSteps().get(0).getNextSteps();
    assertNull(thirdLevelSteps.get(0).getNextSteps());
    assertEquals(1, thirdLevelSteps.get(0).getAllSteps().size());
    assertNull(thirdLevelSteps.get(1).getNextSteps());
    assertEquals(1, thirdLevelSteps.get(1).getAllSteps().size());
  }

  private static PipelineProcess createMockProcess(UUID datasetKey, int attempt) {
    PipelineProcess process = new PipelineProcess();
    process.setDatasetKey(datasetKey);
    process.setAttempt(attempt);

    // add steps
    PipelineStep s1 = new PipelineStep();
    s1.setName(StepType.ABCD_TO_VERBATIM);
    s1.setState(Status.FAILED);
    s1.setStarted(LocalDateTime.now().minusMinutes(30));
    process.addStep(s1);

    PipelineStep s2 = new PipelineStep();
    s2.setName(StepType.ABCD_TO_VERBATIM);
    s2.setState(Status.COMPLETED);
    s2.setStarted(LocalDateTime.now().minusMinutes(29));
    process.addStep(s2);

    PipelineStep s3 = new PipelineStep();
    s3.setName(StepType.VERBATIM_TO_INTERPRETED);
    s3.setState(Status.COMPLETED);
    s3.setStarted(LocalDateTime.now().minusMinutes(28));
    process.addStep(s3);

    PipelineStep s4 = new PipelineStep();
    s4.setName(StepType.INTERPRETED_TO_INDEX);
    s4.setState(Status.COMPLETED);
    s4.setStarted(LocalDateTime.now().minusMinutes(27));
    process.addStep(s4);

    PipelineStep s5 = new PipelineStep();
    s5.setName(StepType.HIVE_VIEW);
    s5.setState(Status.COMPLETED);
    s5.setStarted(LocalDateTime.now().minusMinutes(27));
    process.addStep(s5);

    return process;
  }
}
