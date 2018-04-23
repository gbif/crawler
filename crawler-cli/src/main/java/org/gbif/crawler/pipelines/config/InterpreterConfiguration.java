package org.gbif.crawler.pipelines.config;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.AvroWriteConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

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
  @NotNull
  @Min(1)
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

  @Parameter(names = "--spark-driver-emory")
  @NotNull
  public String sparkDriverMemory;

  @Parameter(names = "--direct-parallelism")
  @NotNull
  @Min(1)
  public int directParallelism;

  @Parameter(names = "--direct-stack-size")
  @NotNull
  public String directStackSize;

  @Parameter(names = "--direct-heap-size")
  @NotNull
  public String directHeapSize;

  @Parameter(names = "--switch-file-size")
  @NotNull
  @Min(1)
  public long switchFileSize;

  @Parameter(names = "--jar-full-path")
  @NotNull
  public String jarFullPath;

  @Parameter(names = "--main-class")
  @NotNull
  public String mainClass;

  @Parameter(names = "--target-directory")
  @NotNull
  public String targetDirectory;

  @Parameter(names = "--process-error-directory")
  public String processErrorDirectory;

  @Parameter(names = "--process-output-directory")
  public String processOutputDirectory;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("messaging", messaging)
      .add("queueName", queueName)
      .add("poolSize", poolSize)
      .add("avroConfig", avroConfig)
      .add("hdfsSiteConfig", hdfsSiteConfig)
      .add("taxonWsConfig", wsConfig)
      .add("otherUser", otherUser)
      .add("directParallelism", directParallelism)
      .add("directStackSize", directStackSize)
      .add("directHeapSize", directHeapSize)
      .add("sparkParallelism", sparkParallelism)
      .add("sparkMemoryOverhead", sparkMemoryOverhead)
      .add("sparkExecutorMemory", sparkExecutorMemory)
      .add("sparkExecutorCores", sparkExecutorCores)
      .add("sparkExecutorNumbers", sparkExecutorNumbers)
      .add("jarFullPath", jarFullPath)
      .add("targetDirectory", targetDirectory)
      .add("mainClass", mainClass)
      .add("processErrorDirectory", processErrorDirectory)
      .add("processOutputDirectory", processOutputDirectory)
      .toString();
  }
}
