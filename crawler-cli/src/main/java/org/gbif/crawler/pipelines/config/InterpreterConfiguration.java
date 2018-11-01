package org.gbif.crawler.pipelines.config;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.AvroWriteConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.MoreObjects;

/**
 * Configuration required to start Interpretation Pipeline on provided dataset
 */
public class InterpreterConfiguration {

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

  @Parameter(names = "--ws-config")
  @Valid
  @NotNull
  public String wsConfig;

  @Parameter(names = "--other-user")
  public String otherUser;

  @Parameter(names = "--spark-parallelism")
  public int sparkParallelism;

  @Parameter(names = "--spark-memory-overhead")
  @NotNull
  @Min(1)
  public int sparkMemoryOverhead;

  @Parameter(names = "--spark-executor-memory")
  @NotNull
  public String sparkExecutorMemory;

  @Parameter(names = "--spark-executor-cores")
  @NotNull
  public int sparkExecutorCores;

  @Parameter(names = "--spark-executor-numbers")
  @NotNull
  public int sparkExecutorNumbers;

  @Parameter(names = "--spark-driver-memory")
  @NotNull
  public String sparkDriverMemory;

  @Parameter(names = "--standalone-stack-size")
  @NotNull
  public String standaloneStackSize;

  @Parameter(names = "--standalone-heap-size")
  @NotNull
  public String standaloneHeapSize;

  @Parameter(names = "--switch-file-size-mb")
  @NotNull
  @Min(1)
  public int switchFileSizeMb;

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

  @Parameter(names = "--target-directory")
  @NotNull
  public String targetDirectory;

  @Parameter(names = "--process-error-directory")
  public String processErrorDirectory;

  @Parameter(names = "--process-output-directory")
  public String processOutputDirectory;

  @Parameter(names = "--log-config-path")
  public String logConfigPath;

  @Parameter(names = "--driver-java-options")
  public String driverJavaOptions;

  @Parameter(names = "--metrics-properties-path")
  public String metricsPropertiesPath;

  @Parameter(names = "--extra-class-path")
  public String extraClassPath;

  @Parameter(names = "--deploy-mode")
  public String deployMode;

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("messaging", messaging)
      .add("queueName", queueName)
      .add("poolSize", poolSize)
      .add("avroConfig", avroConfig)
      .add("hdfsSiteConfig", hdfsSiteConfig)
      .add("wsConfig", wsConfig)
      .add("otherUser", otherUser)
      .add("standaloneStackSize", standaloneStackSize)
      .add("standaloneHeapSize", standaloneHeapSize)
      .add("sparkParallelism", sparkParallelism)
      .add("sparkMemoryOverhead", sparkMemoryOverhead)
      .add("sparkExecutorMemory", sparkExecutorMemory)
      .add("sparkExecutorCores", sparkExecutorCores)
      .add("sparkExecutorNumbers", sparkExecutorNumbers)
      .add("standaloneJarPath", standaloneJarPath)
      .add("distributedJarPath", distributedJarPath)
      .add("targetPath", targetDirectory)
      .add("distributedMainClass", distributedMainClass)
      .add("standaloneMainClass", standaloneMainClass)
      .add("processErrorDirectory", processErrorDirectory)
      .add("processOutputDirectory", processOutputDirectory)
      .add("logConfigPath", logConfigPath)
      .add("metricsPropertiesPath", metricsPropertiesPath)
      .add("extraClassPath", extraClassPath)
      .add("driverJavaOptions", driverJavaOptions)
      .toString();
  }
}
