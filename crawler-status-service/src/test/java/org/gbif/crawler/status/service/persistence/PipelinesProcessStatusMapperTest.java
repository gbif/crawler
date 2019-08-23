package org.gbif.crawler.status.service.persistence;

import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;

import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import static org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep;
import static org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep.MetricInfo;
import static org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.PipelinesStep.Status;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link PipelinesProcessMapper}.
 */
public class PipelinesProcessStatusMapperTest extends BaseMapperTest {

  private static PipelinesProcessMapper pipelinesProcessMapper;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void injectDependencies() {
    pipelinesProcessMapper = injector.getInstance(PipelinesProcessMapper.class);
  }

  @Test
  public void createPipelinesProcessTest() {
    // create process
    PipelinesProcessStatus process =
      new PipelinesProcessStatus().setDatasetKey(UUID.randomUUID()).setAttempt(1).setDatasetTitle("title");

    // insert in the DB
    pipelinesProcessMapper.create(process);
    assertTrue(process.getKey() > 0);

    // get process inserted
    PipelinesProcessStatus processRetrieved = pipelinesProcessMapper.get(process.getDatasetKey(), process.getAttempt());
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
    PipelinesProcessStatus process = new PipelinesProcessStatus().setDatasetKey(UUID.randomUUID()).setAttempt(1);
    pipelinesProcessMapper.create(process);

    // insert another process with the same datasetKey and attempt
    PipelinesProcessStatus duplicate =
      new PipelinesProcessStatus().setDatasetKey(process.getDatasetKey()).setAttempt(process.getAttempt());
    pipelinesProcessMapper.create(process);
  }

  @Test
  public void addStepTest() {
    // insert one process
    PipelinesProcessStatus process = new PipelinesProcessStatus().setDatasetKey(UUID.randomUUID()).setAttempt(1);
    pipelinesProcessMapper.create(process);

    // add a step
    PipelinesStep step = new PipelinesStep().setName("test name")
      .setRunner("runner test")
      .setState(Status.COMPLETED)
      .setStarted(LocalDateTime.now().minusMinutes(1))
      .setFinished(LocalDateTime.now())
      .setMessage("message")
      .setMetrics(Collections.singleton(new MetricInfo("n", "v")));

    pipelinesProcessMapper.addPipelineStep(process.getKey(), step);

    // assert results
    PipelinesProcessStatus processRetrieved = pipelinesProcessMapper.get(process.getDatasetKey(), process.getAttempt());
    assertEquals(process.getDatasetKey(), processRetrieved.getDatasetKey());
    assertEquals(process.getAttempt(), processRetrieved.getAttempt());
    assertEquals(1, processRetrieved.getSteps().size());
    assertEquals(step, processRetrieved.getSteps().iterator().next());
  }

}
