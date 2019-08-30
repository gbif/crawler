package org.gbif.crawler.status.service.persistence;

import org.gbif.api.model.crawler.pipelines.PipelineProcess;
import org.gbif.api.model.crawler.pipelines.PipelineStep;
import org.gbif.api.model.crawler.pipelines.StepRunner;
import org.gbif.api.model.crawler.pipelines.StepType;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;

import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import static org.gbif.api.model.crawler.pipelines.PipelineStep.MetricInfo;
import static org.gbif.api.model.crawler.pipelines.PipelineStep.Status;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the {@link PipelineProcessMapper}. */
public class PipelineProcessMapperTest extends BaseMapperTest {

  private static final String TEST_USER = "test";
  private static final String UPDATER_USER = "updater";

  private static PipelineProcessMapper pipelineProcessMapper;

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void injectDependencies() {
    pipelineProcessMapper = injector.getInstance(PipelineProcessMapper.class);
  }

  @Test
  public void createPipelinesProcessTest() {
    // create process
    PipelineProcess process =
        new PipelineProcess()
            .setDatasetKey(UUID.randomUUID())
            .setAttempt(1)
            .setDatasetTitle("title")
            .setCreatedBy(TEST_USER);

    // insert in the DB
    pipelineProcessMapper.create(process);
    assertTrue(process.getKey() > 0);

    // get process inserted
    PipelineProcess processRetrieved =
        pipelineProcessMapper.get(process.getDatasetKey(), process.getAttempt());
    assertEquals(process.getDatasetKey(), processRetrieved.getDatasetKey());
    assertEquals(process.getAttempt(), processRetrieved.getAttempt());
    assertEquals(process.getDatasetTitle(), processRetrieved.getDatasetTitle());
    assertTrue(process.getSteps().isEmpty());
  }

  @Test
  public void duplicatePipelinesProcessTest() {
    expectedException.expect(PersistenceException.class);
    expectedException.expectCause(isA(PSQLException.class));

    // insert one process
    PipelineProcess process =
        new PipelineProcess()
            .setDatasetKey(UUID.randomUUID())
            .setAttempt(1)
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.create(process);

    // insert another process with the same datasetKey and attempt
    PipelineProcess duplicate =
        new PipelineProcess()
            .setDatasetKey(process.getDatasetKey())
            .setAttempt(process.getAttempt())
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.create(process);
  }

  @Test
  public void addStepTest() {
    // insert one process
    PipelineProcess process =
        new PipelineProcess()
            .setDatasetKey(UUID.randomUUID())
            .setAttempt(1)
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.create(process);

    // add a step
    PipelineStep step =
        new PipelineStep()
            .setType(StepType.ABCD_TO_VERBATIM)
            .setRunner(StepRunner.STANDALONE)
            .setState(Status.COMPLETED)
            .setStarted(LocalDateTime.now().minusMinutes(1))
            .setFinished(LocalDateTime.now())
            .setMessage("message")
            .setMetrics(Collections.singleton(new MetricInfo("n", "v")))
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.addPipelineStep(process.getKey(), step);
    assertTrue(step.getKey() > 0);

    // assert results
    PipelineProcess processRetrieved =
        pipelineProcessMapper.get(process.getDatasetKey(), process.getAttempt());
    assertEquals(process.getDatasetKey(), processRetrieved.getDatasetKey());
    assertEquals(process.getAttempt(), processRetrieved.getAttempt());
    assertEquals(1, processRetrieved.getSteps().size());
    assertTrue(step.lenientEquals(processRetrieved.getSteps().iterator().next()));
  }

  @Test
  public void listAndCountTest() {
    // insert some processes
    final UUID uuid1 = UUID.randomUUID();
    final UUID uuid2 = UUID.randomUUID();
    pipelineProcessMapper.create(
        new PipelineProcess().setDatasetKey(uuid1).setAttempt(1).setCreatedBy(TEST_USER));
    pipelineProcessMapper.create(
        new PipelineProcess().setDatasetKey(uuid1).setAttempt(2).setCreatedBy(TEST_USER));
    pipelineProcessMapper.create(
        new PipelineProcess().setDatasetKey(uuid2).setAttempt(1).setCreatedBy(TEST_USER));

    // list processes
    assertListResult(null, null, 3);
    assertListResult(uuid1, null, 2);
    assertListResult(uuid2, null, 1);
    assertListResult(uuid2, 1, 1);
    assertListResult(uuid2, 10, 0);
    assertListResult(null, 1, 2);
    assertListResult(null, 10, 0);
  }

  private void assertListResult(UUID datasetKey, Integer attempt, int expectedResult) {
    assertEquals(expectedResult, pipelineProcessMapper.count(datasetKey, attempt));
    assertEquals(
        expectedResult, pipelineProcessMapper.list(datasetKey, attempt, DEFAULT_PAGE).size());
  }

  @Test
  public void getPipelineStepTest() {
    // insert one process
    PipelineProcess process =
        new PipelineProcess()
            .setDatasetKey(UUID.randomUUID())
            .setAttempt(1)
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.create(process);

    // add a step
    PipelineStep step =
        new PipelineStep()
            .setType(StepType.ABCD_TO_VERBATIM)
            .setState(Status.RUNNING)
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.addPipelineStep(process.getKey(), step);

    // get step
    PipelineStep stepRetrieved = pipelineProcessMapper.getPipelineStep(step.getKey());
    assertTrue(stepRetrieved.lenientEquals(step));
  }

  @Test
  public void updatePipelineStepTest() {
    // insert one process
    PipelineProcess process =
        new PipelineProcess()
            .setDatasetKey(UUID.randomUUID())
            .setAttempt(1)
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.create(process);

    // add a step
    PipelineStep step =
        new PipelineStep()
            .setType(StepType.ABCD_TO_VERBATIM)
            .setStarted(LocalDateTime.now())
            .setState(Status.RUNNING)
            .setCreatedBy(TEST_USER);
    pipelineProcessMapper.addPipelineStep(process.getKey(), step);
    assertEquals(Status.RUNNING, pipelineProcessMapper.getPipelineStep(step.getKey()).getState());

    // change step state
    step.setFinished(LocalDateTime.now().plusHours(1));
    step.setState(Status.COMPLETED);
    step.setModifiedBy(UPDATER_USER);
    step.setRerunReason("reason");

    pipelineProcessMapper.updatePipelineStep(step);
    assertTrue(pipelineProcessMapper.getPipelineStep(step.getKey()).lenientEquals(step));
  }

  @Test
  public void getLastAttemptTest() {
    final UUID uuid1 = UUID.randomUUID();

    // shouldn't find any attempt
    assertFalse(pipelineProcessMapper.getLastAttempt(uuid1).isPresent());

    // insert some processes
    pipelineProcessMapper.create(
        new PipelineProcess().setDatasetKey(uuid1).setAttempt(1).setCreatedBy(TEST_USER));
    pipelineProcessMapper.create(
        new PipelineProcess().setDatasetKey(uuid1).setAttempt(2).setCreatedBy(TEST_USER));

    // get last attempt
    assertEquals(2, pipelineProcessMapper.getLastAttempt(uuid1).get().intValue());

    // add new attempt
    pipelineProcessMapper.create(
        new PipelineProcess().setDatasetKey(uuid1).setAttempt(3).setCreatedBy(TEST_USER));
    assertEquals(3, pipelineProcessMapper.getLastAttempt(uuid1).get().intValue());
  }
}
