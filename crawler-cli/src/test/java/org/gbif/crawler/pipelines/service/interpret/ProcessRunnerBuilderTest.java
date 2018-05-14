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
      "java -XX:+UseG1GC -Xms1G -Xmx1G -Dlogback.configurationFile=file.xml -cp java.jar org.gbif.Test --wsProperties=/path/ws.config --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d "
      + "--attempt=1 --interpretationTypes=ALL --runner=DirectRunner --defaultTargetDirectory=tmp --inputFile=verbatim.avro "
      + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml";

    RunnerEnum runner = RunnerEnum.DIRECT;
    String datasetId = "de7ffb5e-c07b-42dc-8a88-f67a4465fe3d";
    int attempt = 1;
    String jarFullPath = "java.jar";
    String mainClass = "org.gbif.Test";
    String type = "ALL";
    String input = "verbatim.avro";
    String output = "tmp";
    String avroType = "SNAPPY";
    int avroSync = 1;
    String wsConfig = "/path/ws.config";
    String xmx = "1G";
    String xms = "1G";
    String core = "core.xml";
    String hdfs = "hdfs.xml";
    String log = "file.xml";

    // Expected
    ProcessBuilder builder = ProcessRunnerBuilder.create()
      .runner(runner)
      .datasetId(datasetId)
      .attempt(attempt)
      .jarFullPath(jarFullPath)
      .mainClass(mainClass)
      .interpretationTypes(type)
      .inputFile(input)
      .targetDirectory(output)
      .avroCompressionType(avroType)
      .avroSyncInterval(avroSync)
      .wsConfig(wsConfig)
      .directStackSize(xms)
      .directHeapSize(xmx)
      .coreSiteConfig(core)
      .hdfsSiteConfig(hdfs)
      .logConfigPath(log)
      .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommand() {
    // When
    String expected =
      "spark2-submit --properties-file /path/ws.config --conf spark.default.parallelism=1 --conf spark.yarn.executor.memoryOverhead=1 --class org.gbif.Test "
      + "--master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar --wsProperties=ws.config"
      + " --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL --runner=SparkRunner --defaultTargetDirectory=tmp "
      + "--inputFile=verbatim.avro --avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml";

    RunnerEnum runner = RunnerEnum.SPARK;
    String datasetId = "de7ffb5e-c07b-42dc-8a88-f67a4465fe3d";
    int attempt = 1;
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
    String wsConfig = "/path/ws.config";
    String driverMemory = "4G";
    String core = "core.xml";
    String hdfs = "hdfs.xml";

    // Expected
    ProcessBuilder builder = ProcessRunnerBuilder.create()
      .runner(runner)
      .datasetId(datasetId)
      .attempt(attempt)
      .jarFullPath(jarFullPath)
      .mainClass(mainClass)
      .interpretationTypes(type)
      .inputFile(input)
      .targetDirectory(output)
      .sparkParallelism(sparkParallelism)
      .sparkMemoryOverhead(memoryOverhead)
      .sparkExecutorCores(executorCores)
      .sparkExecutorMemory(executorMemory)
      .sparkExecutorNumbers(executorNumbers)
      .sparkDriverMemory(driverMemory)
      .avroCompressionType(avroType)
      .avroSyncInterval(avroSync)
      .wsConfig(wsConfig)
      .coreSiteConfig(core)
      .hdfsSiteConfig(hdfs)
      .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

}