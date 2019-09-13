package org.gbif.crawler.pipelines.interpret;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.AvroWriteConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.MoreObjects;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration required to start Interpretation Pipeline on provided dataset
 */
public class InterpreterConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--pool-size")
  @NotNull
  @Min(1)
  public int poolSize;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.VERBATIM_TO_INTERPRETED + ".yml";

  @ParametersDelegate
  @Valid
  @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--hdfs-site-config")
  @NotNull
  public String hdfsSiteConfig;

  @Parameter(names = "--core-site-config")
  @NotNull
  public String coreSiteConfig;

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Parameter(names = "--other-user")
  public String otherUser;

  @Parameter(names = "--spark-parallelism-min")
  public int sparkParallelismMin;

  @Parameter(names = "--spark-parallelism-max")
  public int sparkParallelismMax;

  @Parameter(names = "--spark-records-per-thread")
  public int sparkRecordsPerThread;

  @Parameter(names = "--spark-memory-overhead")
  @NotNull
  @Min(1)
  public int sparkMemoryOverhead;

  @Parameter(names = "--spark-executor-memory-gb-min")
  @NotNull
  public int sparkExecutorMemoryGbMin;

  @Parameter(names = "--spark-executor-memory-gb-max")
  @NotNull
  public int sparkExecutorMemoryGbMax;

  @Parameter(names = "--spark-executor-cores")
  @NotNull
  public int sparkExecutorCores;

  @Parameter(names = "--spark-executor-numbers-min")
  @NotNull
  @Min(1)
  public int sparkExecutorNumbersMin;

  @Parameter(names = "--spark-executor-numbers-max")
  @NotNull
  public int sparkExecutorNumbersMax;

  @Parameter(names = "--spark-driver-memory")
  @NotNull
  public String sparkDriverMemory;

  @Parameter(names = "--standalone-stack-size")
  @NotNull
  public String standaloneStackSize;

  @Parameter(names = "--standalone-heap-size")
  @NotNull
  public String standaloneHeapSize;

  @Parameter(names = "--distributed-jar-path")
  @NotNull
  public String distributedJarPath;

  @Parameter(names = "--standalone-jar-path")
  @NotNull
  public String standaloneJarPath;

  @Parameter(names = "--standalone-main-class")
  @NotNull
  public String standaloneMainClass;

  @Parameter(names = "--distributed-main-class")
  @NotNull
  public String distributedMainClass;

  @Parameter(names = "--repository-path")
  @NotNull
  public String repositoryPath;

  @Parameter(names = "--process-error-directory")
  public String processErrorDirectory;

  @Parameter(names = "--process-output-directory")
  public String processOutputDirectory;

  @Parameter(names = "--driver-java-options")
  public String driverJavaOptions;

  @Parameter(names = "--metrics-properties-path")
  public String metricsPropertiesPath;

  @Parameter(names = "--extra-class-path")
  public String extraClassPath;

  @Parameter(names = "--deploy-mode")
  public String deployMode;

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

  @Parameter(names = "--yarn-queue")
  public String yarnQueue;

  @ParametersDelegate
  @NotNull
  @Valid
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("messaging", messaging)
        .add("queueName", queueName)
        .add("poolSize", poolSize)
        .add("avroConfig", avroConfig)
        .add("hdfsSiteConfig", hdfsSiteConfig)
        .add("pipelinesConfig", pipelinesConfig)
        .add("otherUser", otherUser)
        .add("standaloneStackSize", standaloneStackSize)
        .add("standaloneHeapSize", standaloneHeapSize)
        .add("sparkMemoryOverhead", sparkMemoryOverhead)
        .add("sparkExecutorMemoryGbMin", sparkExecutorMemoryGbMin)
        .add("sparkExecutorMemoryGbMax", sparkExecutorMemoryGbMax)
        .add("sparkExecutorCores", sparkExecutorCores)
        .add("sparkExecutorNumbersMin", sparkExecutorNumbersMin)
        .add("sparkExecutorNumbersMax", sparkExecutorNumbersMax)
        .add("standaloneJarPath", standaloneJarPath)
        .add("distributedJarPath", distributedJarPath)
        .add("repositoryPath", repositoryPath)
        .add("distributedMainClass", distributedMainClass)
        .add("standaloneMainClass", standaloneMainClass)
        .add("processErrorDirectory", processErrorDirectory)
        .add("processOutputDirectory", processOutputDirectory)
        .add("metricsPropertiesPath", metricsPropertiesPath)
        .add("extraClassPath", extraClassPath)
        .add("driverJavaOptions", driverJavaOptions)
        .add("sparkRecordsPerThread", sparkRecordsPerThread)
        .add("metaFileName", metaFileName)
        .add("processRunner", processRunner)
        .add("sparkParallelismMax", sparkParallelismMax)
        .add("sparkParallelismMin", sparkParallelismMin)
        .add("yarnQueue", yarnQueue)
        .add("registry", registry)
        .toString();
  }
}
