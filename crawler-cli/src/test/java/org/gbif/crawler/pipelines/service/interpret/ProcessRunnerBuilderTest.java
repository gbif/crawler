package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.crawler.pipelines.service.interpret.ProcessRunnerBuilder.RunnerEnum;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProcessRunnerBuilderTest {

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyRunner() {
    // Should
    ProcessRunnerBuilder.create().build();
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyParameters() {
    // When
    RunnerEnum runner = RunnerEnum.DIRECT;

    // Should
    ProcessRunnerBuilder.create().runner(runner).build();
  }

  @Test
  public void testDirectRunnerCommand() {
    // When
    String expected =
      "java -cp java.jar org.gbif.Test --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --interpretationTypes=ALL "
      + "--runner=DirectRunner --defaultTargetDirectory=tmp --inputFile=verbatim.avro --avroCompressionType=SNAPPY "
      + "--avroSyncInterval=1 --wsProperties=ws.config";

    RunnerEnum runner = RunnerEnum.DIRECT;
    String datasetId = "de7ffb5e-c07b-42dc-8a88-f67a4465fe3d";
    String jarFullPath = "java.jar";
    String mainClass = "org.gbif.Test";
    String type = "ALL";
    String input = "verbatim.avro";
    String output = "tmp";
    String avroType = "SNAPPY";
    int avroSync = 1;
    String wsConfig = "ws.config";

    // Expected
    ProcessBuilder builder = ProcessRunnerBuilder.create()
      .runner(runner)
      .datasetId(datasetId)
      .jarFullPath(jarFullPath)
      .mainClass(mainClass)
      .interpretationTypes(type)
      .inputFile(input)
      .targetDirectory(output)
      .avroCompressionType(avroType)
      .avroSyncInterval(avroSync)
      .wsConfig(wsConfig)
      .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommand() {
    // When
    String expected =
      "spark-submit --conf spark.default.parallelism=1 --conf spark.yarn.executor.memoryOverhead=1 --class org.gbif.Test "
      + "--master yarn --executor-memory 1G --executor-cores 1 --num-executors 1 java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d "
      + "--interpretationTypes=ALL --runner=SparkRunner --defaultTargetDirectory=tmp --inputFile=verbatim.avro --avroCompressionType=SNAPPY "
      + "--avroSyncInterval=1 --wsProperties=ws.config";

    RunnerEnum runner = RunnerEnum.SPARK;
    String datasetId = "de7ffb5e-c07b-42dc-8a88-f67a4465fe3d";
    String jarFullPath = "java.jar";
    String mainClass = "org.gbif.Test";
    String type = "ALL";
    String input = "verbatim.avro";
    String output = "tmp";
    String executorMemory = "1G";
    Integer executorCores = 1;
    Integer executorNumbers = 1;
    Integer sparkParallelism = 1;
    Integer memoryOverhead = 1;
    String avroType = "SNAPPY";
    int avroSync = 1;
    String wsConfig = "ws.config";

    // Expected
    ProcessBuilder builder = ProcessRunnerBuilder.create()
      .runner(runner)
      .datasetId(datasetId)
      .jarFullPath(jarFullPath)
      .mainClass(mainClass)
      .interpretationTypes(type)
      .inputFile(input)
      .targetDirectory(output)
      .sparkParallelism(sparkParallelism)
      .memoryOverhead(memoryOverhead)
      .executorCores(executorCores)
      .executorMemory(executorMemory)
      .executorNumbers(executorNumbers)
      .avroCompressionType(avroType)
      .avroSyncInterval(avroSync)
      .wsConfig(wsConfig)
      .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

}