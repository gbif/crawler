package org.gbif.crawler.pipelines.interpret;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.crawler.pipelines.PipelineCallback.Runner;

import org.junit.Test;

import static org.gbif.crawler.pipelines.PipelineCallback.Steps.ALL;
import static org.junit.Assert.assertEquals;

public class ProcessRunnerBuilderTest {

  @Test(expected = NullPointerException.class)
  public void testEmptyRunner() {
    // Should
    ProcessRunnerBuilder.create().build();
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyParameters() {

    // Should
    ProcessRunnerBuilder.create().build();
  }

  @Test
  public void testDirectRunnerCommand() {
    // State
    String expected =
        "java -XX:+UseG1GC -Xms1G -Xmx1G -Dlog4j.configuration=file:/home/crap/config/log4j-interpretation-pipeline.properties "
            + "-cp java.jar org.gbif.Test --pipelineStep=VERBATIM_TO_INTERPRETED --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 "
            + "--interpretationTypes=ALL --runner=SparkRunner --targetPath=tmp --metaFileName=verbatim-to-interpreted.yml --inputPath=verbatim.avro "
            + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--properties=/path/ws.config --endPointType=DWC_ARCHIVE --tripletValid=true --occurrenceIdValid=true";

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
    config.driverJavaOptions = "-Dlog4j.configuration=file:/home/crap/config/log4j-interpretation-pipeline.properties";
    config.processRunner = Runner.STANDALONE.name();

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(ALL.name());
    Set<String> steps = Collections.singleton(ALL.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(datasetId, attempt, types, steps, null, EndpointType.DWC_ARCHIVE, null,
            new ValidationResult(true, true, null, 100L));

    // When
    ProcessBuilder builder =
        ProcessRunnerBuilder.create().config(config).message(message).inputPath("verbatim.avro").build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommand() {
    // When
    String expected =
        "spark2-submit --conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.yarn.maxAppAttempts=1 "
            + "--conf spark.dynamicAllocation.enabled=false "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL "
            + "--runner=SparkRunner --targetPath=tmp --metaFileName=verbatim-to-interpreted.yml --inputPath=verbatim.avro "
            + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--properties=/path/ws.config --endPointType=DWC_ARCHIVE --tripletValid=true --occurrenceIdValid=true --useExtendedRecordId=true";

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.sparkExecutorMemoryGbMax = 10;
    config.sparkExecutorMemoryGbMin = 1;
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbersMin = 1;
    config.sparkExecutorNumbersMax = 2;
    config.sparkMemoryOverhead = 1;
    config.avroConfig.compressionType = "SNAPPY";
    config.avroConfig.syncInterval = 1;
    config.wsConfig = "/path/ws.config";
    config.sparkDriverMemory = "4G";
    config.coreSiteConfig = "core.xml";
    config.hdfsSiteConfig = "hdfs.xml";
    config.deployMode = "cluster";
    config.processRunner = Runner.DISTRIBUTED.name();

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(ALL.name());
    Set<String> steps = Collections.singleton(ALL.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(datasetId, attempt, types, steps, null, EndpointType.DWC_ARCHIVE, "something",
            new ValidationResult(true, true, true, null));

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .inputPath("verbatim.avro")
            .sparkParallelism(1)
            .sparkExecutorMemory("1G")
            .sparkExecutorNumbers(1)
            .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommandFull() {
    // When
    String expected =
        "spark2-submit --conf spark.metrics.conf=metrics.properties --conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines --conf spark.default.parallelism=1 "
            + "--conf spark.executor.memoryOverhead=1 --conf spark.yarn.maxAppAttempts=1 --conf spark.dynamicAllocation.enabled=false "
            + "--class org.gbif.Test --master yarn "
            + "--deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL --runner=SparkRunner "
            + "--targetPath=tmp --metaFileName=verbatim-to-interpreted.yml --inputPath=verbatim.avro --avroCompressionType=SNAPPY "
            + "--avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml --properties=/path/ws.config --endPointType=DWC_ARCHIVE";

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.sparkExecutorMemoryGbMax = 10;
    config.sparkExecutorMemoryGbMin = 1;
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbersMin = 1;
    config.sparkExecutorNumbersMax = 2;
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
    config.processRunner = Runner.DISTRIBUTED.name();
    config.yarnQueue = "pipelines";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(ALL.name());
    Set<String> steps = Collections.singleton(ALL.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(datasetId, attempt, types, steps, null, EndpointType.DWC_ARCHIVE, null, null);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .inputPath("verbatim.avro")
            .sparkParallelism(1)
            .sparkExecutorMemory("1G")
            .sparkExecutorNumbers(1)
            .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

}
