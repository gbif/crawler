package org.gbif.crawler.pipelines.hive;

import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to build an instance of ProcessBuilder for direct or spark command
 */
final class ProcessRunnerBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessRunnerBuilder.class);

  private static final String DELIMITER = " ";

  private HiveViewConfiguration config;
  private PipelinesInterpretedMessage message;
  private int sparkParallelism;
  private int sparkExecutorNumbers;
  private String sparkExecutorMemory;

  ProcessRunnerBuilder config(HiveViewConfiguration config) {
    this.config = Objects.requireNonNull(config);
    return this;
  }

  ProcessRunnerBuilder message(PipelinesInterpretedMessage message) {
    this.message = Objects.requireNonNull(message);
    return this;
  }

  ProcessRunnerBuilder sparkParallelism(int sparkParallelism) {
    this.sparkParallelism = sparkParallelism;
    return this;
  }

  ProcessRunnerBuilder sparkExecutorNumbers(int sparkExecutorNumbers) {
    this.sparkExecutorNumbers = sparkExecutorNumbers;
    return this;
  }

  ProcessRunnerBuilder sparkExecutorMemory(String sparkExecutorMemory) {
    this.sparkExecutorMemory = sparkExecutorMemory;
    return this;
  }

  static ProcessRunnerBuilder create() {
    return new ProcessRunnerBuilder();
  }

  ProcessBuilder build() {
    return buildSpark();
  }


  /**
   * Builds ProcessBuilder to process spark command
   */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark2-submit");

    Optional.ofNullable(config.metricsPropertiesPath).ifPresent(x -> joiner.add("--conf spark.metrics.conf=" + x));
    Optional.ofNullable(config.extraClassPath)
        .ifPresent(x -> joiner.add("--conf \"spark.driver.extraClassPath=" + x + "\""));
    Optional.ofNullable(config.driverJavaOptions).ifPresent(x -> joiner.add("--driver-java-options \"" + x + "\""));
    Optional.ofNullable(config.yarnQueue).ifPresent(x -> joiner.add("--queue " + x));

    if (sparkParallelism < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    joiner.add("--conf spark.default.parallelism=" + sparkParallelism)
        .add("--conf spark.executor.memoryOverhead=" + config.sparkMemoryOverhead)
        .add("--conf spark.yarn.maxAppAttempts=1")
        .add("--conf spark.dynamicAllocation.enabled=false")
        .add("--conf \"spark.executor.extraJavaOptions=-XX:+UseG1GC\"")
        .add("--class " + Objects.requireNonNull(config.distributedMainClass))
        .add("--master yarn")
        .add("--deploy-mode " + Objects.requireNonNull(config.deployMode))
        .add("--executor-memory " + Objects.requireNonNull(sparkExecutorMemory))
        .add("--executor-cores " + config.sparkExecutorCores)
        .add("--num-executors " + sparkExecutorNumbers)
        .add("--driver-memory " + config.sparkDriverMemory)
        .add(Objects.requireNonNull(config.distributedJarPath));

    return buildCommon(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline options
   */
  private ProcessBuilder buildCommon(StringJoiner command) {

    // Common properties
    command.add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
        .add("--attempt=" + message.getAttempt())
        .add("--runner=SparkRunner")
        .add("--inputPath=" + Objects.requireNonNull(config.repositoryPath))
        .add("--targetPath=" + Objects.requireNonNull(config.repositoryPath))
        .add("--hdfsSiteConfig=" + Objects.requireNonNull(config.hdfsSiteConfig))
        .add("--coreSiteConfig=" + Objects.requireNonNull(config.coreSiteConfig))
        .add("--properties=" + Objects.requireNonNull(config.pipelinesConfig));

    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(config.otherUser).ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    LOG.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    BiFunction<String, String, File> createDirFn = (String type, String path) -> {
      try {
        Files.createDirectories(Paths.get(path));
        File file = new File(path + message.getDatasetUuid() + "_" + message.getAttempt() + "_idx_" + type + ".log");
        LOG.info("{} file - {}", type, file);
        return file;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };
    // The command side outputs
    if (config.processErrorDirectory != null) {
      builder.redirectError(createDirFn.apply("err", config.processErrorDirectory));
    } else {
      builder.redirectError(new File("/dev/null"));
    }

    if (config.processOutputDirectory != null) {
      builder.redirectOutput(createDirFn.apply("out", config.processOutputDirectory));
    } else {
      builder.redirectOutput(new File("/dev/null"));
    }

    return builder;
  }

}
