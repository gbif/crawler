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
  public String hdfsSiteConfig;

  @Parameter(names = "--other-user")
  public String otherUser;

  @Parameter(names = "--spark-parallelism")
  @NotNull
  @Min(1)
  public int sparkParallelism;

  @Parameter(names = "--direct-parallelism")
  @NotNull
  @Min(1)
  public int directParallelism;

  @Parameter(names = "--memory-overhead")
  @NotNull
  @Min(1)
  public int memoryOverhead;

  @Parameter(names = "--executor-memory")
  @NotNull
  public String executorMemory;

  @Parameter(names = "--executor-cores")
  @NotNull
  public int executorCores;

  @Parameter(names = "--executor-numbers")
  @NotNull
  public int executorNumbers;

  @Parameter(names = "--jar-full-path")
  @NotNull
  public String jarFullPath;

  @Parameter(names = "--main-class")
  @NotNull
  public String mainClass;

  @Parameter(names = "--switch-file-size")
  @NotNull
  public long switchFileSize = 10L * 1_024L;

  @Parameter(names = "--target-directory")
  @NotNull
  public String targetDirectory;

  @Parameter(names = "--procces-error-file")
  public String proccesErrorFile;

  @Parameter(names = "--procces-output-file")
  public String proccesOutputFile;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("messaging", messaging)
      .add("queueName", queueName)
      .add("poolSize", poolSize)
      .add("avroConfig", avroConfig)
      .add("hdfsSiteConfig", hdfsSiteConfig)
      .add("otherUser", otherUser)
      .add("sparkParallelism", sparkParallelism)
      .add("directParallelism", directParallelism)
      .add("memoryOverhead", memoryOverhead)
      .add("executorMemory", executorMemory)
      .add("executorCores", executorCores)
      .add("executorNumbers", executorNumbers)
      .add("jarFullPath", jarFullPath)
      .add("targetDirectory", targetDirectory)
      .add("mainClass", mainClass)
      .add("proccesErrorFile", proccesErrorFile)
      .add("proccesOutputFile", proccesOutputFile)
      .toString();
  }
}
