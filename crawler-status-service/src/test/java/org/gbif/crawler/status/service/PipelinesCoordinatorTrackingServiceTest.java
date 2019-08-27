package org.gbif.crawler.status.service;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.status.service.impl.PipelinesCoordinatorTrackingServiceImpl;
import org.gbif.crawler.status.service.model.PipelineProcess;
import org.gbif.crawler.status.service.model.PipelineStep;
import org.gbif.crawler.status.service.model.PipelineWorkflow;
import org.gbif.crawler.status.service.model.StepType;
import org.gbif.crawler.status.service.persistence.PipelineProcessMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PipelinesCoordinatorTrackingServiceTest {

  @Mock private PipelineProcessMapper pipelineProcessMapper;
  @Mock private MessagePublisher messagePublisher;

  private PipelinesHistoryTrackingService trackingService;

  @Before
  public void setup() throws Exception {
    trackingService =
        new PipelinesCoordinatorTrackingServiceImpl(messagePublisher, pipelineProcessMapper);
  }

  @Test
  public void getPipelineWorkflowTest() {
    final UUID uuid = UUID.randomUUID();
    final int attempt = 1;

    // mocks
    PipelineProcess mockProcess = createMockProcess();
    when(pipelineProcessMapper.get(uuid, attempt)).thenReturn(mockProcess);

    PipelineWorkflow workflow = trackingService.getPipelinesWorkflow(uuid, attempt);

    // TODO: assertd
  }

  private List<PipelineStep> getStepsByType(PipelineProcess process, List<StepType> types) {
    return process.getSteps().stream()
        .filter(s -> types.contains(s.getName()))
        .collect(Collectors.toList());
  }

  private static PipelineProcess createMockProcess() {
    PipelineProcess process = new PipelineProcess();
    process.setDatasetKey(UUID.randomUUID());
    process.setAttempt(1);

    // add steps
    PipelineStep s1 = new PipelineStep();
    s1.setName(StepType.ABCD_TO_VERBATIM);
    s1.setState(PipelineStep.Status.FAILED);
    s1.setStarted(LocalDateTime.now().minusMinutes(30));
    process.addStep(s1);

    PipelineStep s2 = new PipelineStep();
    s2.setName(StepType.ABCD_TO_VERBATIM);
    s2.setState(PipelineStep.Status.COMPLETED);
    s2.setStarted(LocalDateTime.now().minusMinutes(29));
    process.addStep(s2);

    PipelineStep s3 = new PipelineStep();
    s3.setName(StepType.VERBATIM_TO_INTERPRETED);
    s3.setState(PipelineStep.Status.COMPLETED);
    s3.setStarted(LocalDateTime.now().minusMinutes(28));
    process.addStep(s3);

    PipelineStep s4 = new PipelineStep();
    s4.setName(StepType.INTERPRETED_TO_INDEX);
    s4.setState(PipelineStep.Status.COMPLETED);
    s4.setStarted(LocalDateTime.now().minusMinutes(27));
    process.addStep(s4);

    PipelineStep s5 = new PipelineStep();
    s5.setName(StepType.HIVE_VIEW);
    s5.setState(PipelineStep.Status.COMPLETED);
    s5.setStarted(LocalDateTime.now().minusMinutes(27));
    process.addStep(s5);

    return process;
  }
}
