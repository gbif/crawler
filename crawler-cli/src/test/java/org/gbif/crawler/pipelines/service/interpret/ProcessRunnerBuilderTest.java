package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.crawler.pipelines.config.InterpreterConfiguration;
import org.gbif.crawler.pipelines.service.interpret.ProcessRunnerBuilder.RunnerEnum;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProcessRunnerBuilderTest {

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyRunner() {
    // Should
    ProcessRunnerBuilder.create().build();
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyParameters() {
    // State
    RunnerEnum runner = RunnerEnum.DISTRIBUTED;

    // Should
    ProcessRunnerBuilder.create().runner(runner).build();
  }

  @Test
  public void testDirectRunnerCommand() {
    // State
    String expected =
      "java -XX:+UseG1GC -Xms1G -Xmx1G -Dlogback.configurationFile=file.xml -cp java.jar org.gbif.Test "
      + "--pipelineStep=VERBATIM_TO_INTERPRETED --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 "
      + "--interpretationTypes=ALL --runner=SparkRunner --targetPath=tmp --inputPath=verbatim.avro "
      + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
      + "--wsProperties=/path/ws.config";

    RunnerEnum runner = RunnerEnum.STANDALONE;

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.standaloneJarPath = "java.jar";
    config.standaloneMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.avroConfig.compressionType = "SNAPPY";
    config.avroConfig.syncInterval = 1;
    config.wsConfig = "/path/ws.config";
    config.standaloneHeapSize = "1G";
    config.standaloneStackSize = "1G";
    config.coreSiteConfig = "core.xml";
    config.hdfsSiteConfig = "hdfs.xml";
    config.logConfigPath = "file.xml";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton("ALL");
    Set<String> steps = Collections.singleton("ALL");
    PipelinesVerbatimMessage message = new PipelinesVerbatimMessage(datasetId, attempt, types, steps);

    // When
    ProcessBuilder builder =
      ProcessRunnerBuilder.create().runner(runner).config(config).message(message).inputPath("verbatim.avro").build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommand() {
    // When
    String expected =
      "spark2-submit --conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 "
      + "--conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true --class org.gbif.Test "
      + "--master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
      + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 "
      + "--interpretationTypes=ALL --runner=SparkRunner --targetPath=tmp --inputPath=verbatim.avro "
      + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
      + "--wsProperties=/path/ws.config";

    RunnerEnum runner = RunnerEnum.DISTRIBUTED;

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.sparkExecutorMemory = "1G";
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbers = 1;
    config.sparkParallelism = 1;
    config.sparkMemoryOverhead = 1;
    config.avroConfig.compressionType = "SNAPPY";
    config.avroConfig.syncInterval = 1;
    config.wsConfig = "/path/ws.config";
    config.sparkDriverMemory = "4G";
    config.coreSiteConfig = "core.xml";
    config.hdfsSiteConfig = "hdfs.xml";
    config.deployMode = "cluster";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton("ALL");
    Set<String> steps = Collections.singleton("ALL");
    PipelinesVerbatimMessage message = new PipelinesVerbatimMessage(datasetId, attempt, types, steps);

    // Expected
    ProcessBuilder builder =
      ProcessRunnerBuilder.create().runner(runner).config(config).message(message).inputPath("verbatim.avro").build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommandFull() {
    // When
    String expected =
      "spark2-submit --conf spark.metrics.conf=metrics.properties --conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
      + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --conf spark.default.parallelism=1 "
      + "--conf spark.executor.memoryOverhead=1 --conf spark.driver.userClassPathFirst=true "
      + "--conf spark.executor.userClassPathFirst=true --class org.gbif.Test "
      + "--master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
      + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 "
      + "--interpretationTypes=ALL --runner=SparkRunner --targetPath=tmp --inputPath=verbatim.avro "
      + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
      + "--wsProperties=/path/ws.config";

    RunnerEnum runner = RunnerEnum.DISTRIBUTED;

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.sparkExecutorMemory = "1G";
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbers = 1;
    config.sparkParallelism = 1;
    config.sparkMemoryOverhead = 1;
    config.avroConfig.compressionType = "SNAPPY";
    config.avroConfig.syncInterval = 1;
    config.wsConfig = "/path/ws.config";
    config.sparkDriverMemory = "4G";
    config.coreSiteConfig = "core.xml";
    config.hdfsSiteConfig = "hdfs.xml";
    config.metricsPropertiesPath = "metrics.properties";
    config.extraClassPath = "logstash-gelf.jar";
    config.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.deployMode = "cluster";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton("ALL");
    Set<String> steps = Collections.singleton("ALL");
    PipelinesVerbatimMessage message = new PipelinesVerbatimMessage(datasetId, attempt, types, steps);

    // Expected
    ProcessBuilder builder =
      ProcessRunnerBuilder.create().runner(runner).config(config).message(message).inputPath("verbatim.avro").build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

}