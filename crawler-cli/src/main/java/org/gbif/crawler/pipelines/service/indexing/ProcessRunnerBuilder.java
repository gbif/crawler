package org.gbif.crawler.pipelines.service.indexing;

import org.gbif.common.messaging.api.messages.IndexDatasetMessage;
import org.gbif.crawler.pipelines.config.IndexingConfiguration;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to build an instance of ProcessBuilder for direct or spark command
 */
final class ProcessRunnerBuilder {

  public enum RunnerEnum {
    STANDALONE, DISTRIBUTED
  }

  private static final Logger LOG = LoggerFactory.getLogger(
    ProcessRunnerBuilder.class);

  private static final String DELIMITER = " ";

  private IndexingConfiguration config;
  private IndexDatasetMessage message;
  private RunnerEnum runner;
  private String esAlias;
  private String esIndexName;
  private String redirectErrorFile;
  private String redirectOutputFile;

  ProcessRunnerBuilder config(IndexingConfiguration config) {
    this.config = Objects.requireNonNull(config);
    return this;
  }

  ProcessRunnerBuilder message(IndexDatasetMessage message) {
    this.message = Objects.requireNonNull(message);
    return this;
  }

  ProcessRunnerBuilder runner(RunnerEnum runner) {
    this.runner = Objects.requireNonNull(runner);
    return this;
  }

  ProcessRunnerBuilder esAlias(String esAlias) {
    this.esAlias = Objects.requireNonNull(esAlias);
    return this;
  }

  ProcessRunnerBuilder esIndexName(String esIndexName) {
    this.esIndexName = Objects.requireNonNull(esIndexName);
    return this;
  }

  ProcessRunnerBuilder redirectErrorFile(String redirectErrorFile) {
    this.redirectErrorFile = redirectErrorFile;
    return this;
  }

  ProcessRunnerBuilder redirectOutputFile(String redirectOutputFile) {
    this.redirectOutputFile = redirectOutputFile;
    return this;
  }

  static ProcessRunnerBuilder create() {
    return new ProcessRunnerBuilder();
  }

  ProcessBuilder build() {
    if (RunnerEnum.STANDALONE == runner) {
      return buildDirect();
    }
    if (RunnerEnum.DISTRIBUTED == runner) {
      return buildSpark();
    }
    throw new IllegalArgumentException("Wrong runner type - " + runner);
  }

  /**
   * Builds ProcessBuilder to process direct command
   */
  private ProcessBuilder buildDirect() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("java")
      .add("-XX:+UseG1GC")
      .add("-Xms" + Objects.requireNonNull(config.standaloneStackSize))
      .add("-Xmx" + Objects.requireNonNull(config.standaloneHeapSize))
      .add("-Dlogback.configurationFile=" + Objects.requireNonNull(config.logConfigPath))
      .add("-cp")
      .add(Objects.requireNonNull(config.standaloneJarPath))
      .add(Objects.requireNonNull(config.standaloneMainClass))
      .add("--pipelineStep=INTERPRETED_TO_ES_INDEX");

    return buildCommon(joiner);
  }

  /**
   * Builds ProcessBuilder to process spark command
   */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark2-submit");

    Optional.ofNullable(config.metricsPropertiesPath).ifPresent(x -> joiner.add("--conf spark.metrics.conf=" + x));
    Optional.ofNullable(config.extraClassPath).ifPresent(x -> joiner.add("--conf \"spark.driver.extraClassPath=" + x + "\""));
    Optional.ofNullable(config.driverJavaOptions).ifPresent(x -> joiner.add("--driver-java-options \"" + x + "\""));

    joiner
      .add("--conf spark.default.parallelism=" + config.sparkParallelism)
      .add("--conf spark.executor.memoryOverhead=" + config.sparkMemoryOverhead)
      .add("--class " + Objects.requireNonNull(config.distributedMainClass))
      .add("--master yarn")
      .add("--deploy-mode cluster")
      .add("--executor-memory " + Objects.requireNonNull(config.sparkExecutorMemory))
      .add("--executor-cores " + config.sparkExecutorCores)
      .add("--num-executors " + config.sparkExecutorNumbers)
      .add("--driver-memory " + config.sparkDriverMemory)
      .add(Objects.requireNonNull(config.distributedJarPath));

    return buildCommon(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline options
   */
  private ProcessBuilder buildCommon(StringJoiner command) {

    String esHosts = String.join(",", config.esHosts);

    // Common properties
    command.add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
      .add("--attempt=" + message.getAttempt())
      .add("--runner=SparkRunner")
      .add("--inputPath=" + Objects.requireNonNull(config.targetDirectory))
      .add("--targetPath=" + Objects.requireNonNull(config.targetDirectory))
      .add("--hdfsSiteConfig=" + Objects.requireNonNull(config.hdfsSiteConfig))
      .add("--coreSiteConfig=" + Objects.requireNonNull(config.coreSiteConfig))
      .add("--esHosts=" + Objects.requireNonNull(esHosts))
      .add("--esAlias=" + Objects.requireNonNull(esAlias))
      .add("--esIndexName=" + Objects.requireNonNull(esIndexName));

    Optional.ofNullable(config.esMaxBatchSizeBytes).ifPresent(x -> command.add("--esMaxBatchSizeBytes=" + x));
    Optional.ofNullable(config.esMaxBatchSize).ifPresent(x -> command.add("--esMaxBatchSize=" + x));
    Optional.ofNullable(config.esSchemaPath).ifPresent(x -> command.add("--esSchemaPath=" + x));
    Optional.ofNullable(config.indexRefreshInterval).ifPresent(x -> command.add("--indexRefreshInterval=" + x));
    Optional.ofNullable(config.indexNumberShards).ifPresent(x -> command.add("--indexNumberShards=" + x));
    Optional.ofNullable(config.indexNumberReplicas).ifPresent(x -> command.add("--indexNumberReplicas=" + x));


    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(config.otherUser).ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    LOG.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    // The command side outputs
    Optional.ofNullable(redirectErrorFile).ifPresent(x -> builder.redirectError(new File(redirectErrorFile)));
    Optional.ofNullable(redirectOutputFile).ifPresent(x -> builder.redirectOutput(new File(redirectOutputFile)));
    return builder;
  }

}
