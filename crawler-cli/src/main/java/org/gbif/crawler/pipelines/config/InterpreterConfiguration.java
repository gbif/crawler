package org.gbif.crawler.pipelines.service.interpret;

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

  @Parameter(names = "--interpreted-record-repository")
  @NotNull
  public String interpretedRecordRepository;

  @ParametersDelegate
  @Valid
  @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--hdfs-site-config")
  public String hdfsSiteConfig;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("messaging", messaging)
      .add("queueName", queueName)
      .add("poolSize", poolSize)
      .add("interpretedRecordRepository", interpretedRecordRepository)
      .add("avroConfig", avroConfig)
      .add("hdfsSiteConfig", hdfsSiteConfig)
      .toString();

//    String user = "hdfs";
//    Integer sparkParallelism = 16;
//    Integer directParallelism = 2;
//    Integer memoryOverhead = 2048;
//    String mainClass = "milestone2.cli.BeamThredControlDemo";
//    String executorMemory = "16G";
//    Integer executorCores = 2;
//    Integer executorNumbers = 3;
//    String jarFullPath = "/home/nvolik/labs-1.1-SNAPSHOT-shaded.jar";
//    String hdfsConfigPath;
//    File error;
//    File output;
//    String defaultTargetDirectory;
  }
}
