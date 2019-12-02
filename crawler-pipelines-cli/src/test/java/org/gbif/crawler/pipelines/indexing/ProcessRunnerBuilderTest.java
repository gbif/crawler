package org.gbif.crawler.pipelines.indexing;

import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

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
        "sudo -u user java -XX:+UseG1GC -Xms1G -Xmx1G -Dlog4j.configuration=file:/home/crap/config/log4j-pipelines.properties "
            + "-cp java.jar org.gbif.Test --pipelineStep=INTERPRETED_TO_ES_INDEX --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d "
            + "--attempt=1 --runner=SparkRunner --inputPath=tmp --targetPath=tmp --metaFileName=interpreted-to-index.yml "
            + "--hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml --esHosts=http://host.com:9300 --properties=/path/ws.config "
            + "--esIndexName=occurrence";

    IndexingConfiguration config = new IndexingConfiguration();
    config.standaloneJarPath = "java.jar";
    config.standaloneMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.standaloneHeapSize = "1G";
    config.standaloneStackSize = "1G";
    config.coreSiteConfig = "core.xml";
    config.hdfsSiteConfig = "hdfs.xml";
    config.driverJavaOptions = "-Dlog4j.configuration=file:/home/crap/config/log4j-pipelines.properties";
    config.processRunner = StepRunner.STANDALONE.name();
    config.esHosts = new String[]{"http://host.com:9300"};
    config.pipelinesConfig = "/path/ws.config";
    config.yarnQueue = "pipelines";
    config.otherUser = "user";
    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage(datasetId, attempt, steps, 100L, false, null);

    String indexName = "occurrence";

    // When
    ProcessBuilder builder =
        ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .esIndexName(indexName)
            .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommand() {
    // When
    String expected = "spark2-submit --conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 "
        + "--conf spark.dynamicAllocation.enabled=false "
        + "--class org.gbif.Test --master yarn --deploy-mode cluster "
        + "--executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
        + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --runner=SparkRunner --inputPath=tmp "
        + "--targetPath=tmp --metaFileName=interpreted-to-index.yml --hdfsSiteConfig=hdfs.xml "
        + "--coreSiteConfig=core.xml --esHosts=http://host.com:9300 --properties=/path/ws.config --esIndexName=occurrence";

    IndexingConfiguration config = new IndexingConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.sparkExecutorMemoryGbMax = 10;
    config.sparkExecutorMemoryGbMin = 1;
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbersMin = 1;
    config.sparkExecutorNumbersMax = 2;
    config.sparkMemoryOverhead = 1;
    config.sparkDriverMemory = "4G";
    config.coreSiteConfig = "core.xml";
    config.hdfsSiteConfig = "hdfs.xml";
    config.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.esHosts = new String[]{"http://host.com:9300"};
    config.pipelinesConfig = "/path/ws.config";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage(datasetId, attempt, steps, null, false, null);

    String indexName = "occurrence";

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .esIndexName(indexName)
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
        "spark2-submit --conf spark.metrics.conf=metrics.properties "
            + "--conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines --conf spark.default.parallelism=1 "
            + "--conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--class org.gbif.Test --master yarn "
            + "--deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --runner=SparkRunner --inputPath=tmp --targetPath=tmp "
            + "--metaFileName=interpreted-to-index.yml --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--esHosts=http://host.com:9300 --properties=/path/ws.config --esIndexName=occurrence";

    IndexingConfiguration config = new IndexingConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.repositoryPath = "tmp";
    config.sparkExecutorMemoryGbMax = 10;
    config.sparkExecutorMemoryGbMin = 1;
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbersMin = 1;
    config.sparkExecutorNumbersMax = 2;
    config.sparkMemoryOverhead = 1;
    config.sparkDriverMemory = "4G";
    config.coreSiteConfig = "core.xml";
    config.hdfsSiteConfig = "hdfs.xml";
    config.metricsPropertiesPath = "metrics.properties";
    config.extraClassPath = "logstash-gelf.jar";
    config.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.esHosts = new String[]{"http://host.com:9300"};
    config.yarnQueue = "pipelines";
    config.pipelinesConfig = "/path/ws.config";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage(datasetId, attempt, steps, 100L, false, null);

    String indexName = "occurrence";

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .sparkParallelism(1)
            .sparkExecutorMemory("1G")
            .sparkExecutorNumbers(1)
            .esIndexName(indexName)
            .build();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

}
