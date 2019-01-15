package org.gbif.crawler.pipelines.interpret;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to build an instance of ProcessBuilder for direct or spark command
 */
final class ProcessRunnerBuilder {

  public enum RunnerEnum {
    STANDALONE,
    DISTRIBUTED
  }

  private static final Logger LOG = LoggerFactory.getLogger(ProcessRunnerBuilder.class);

  private static final String DELIMITER = " ";

  private RunnerEnum runner;
  private InterpreterConfiguration config;
  private PipelinesVerbatimMessage message;
  private int sparkParallelism;
  private String inputPath;

  ProcessRunnerBuilder config(InterpreterConfiguration config) {
    this.config = Objects.requireNonNull(config);
    return this;
  }

  ProcessRunnerBuilder message(PipelinesVerbatimMessage message) {
    this.message = Objects.requireNonNull(message);
    return this;
  }

  ProcessRunnerBuilder runner(RunnerEnum runner) {
    this.runner = Objects.requireNonNull(runner);
    return this;
  }

  ProcessRunnerBuilder sparkParallelism(int sparkParallelism) {
    this.sparkParallelism = sparkParallelism > 0 ? sparkParallelism : config.sparkParallelism;
    return this;
  }

  ProcessRunnerBuilder inputPath(String inputPath) {
    this.inputPath = Objects.requireNonNull(inputPath);
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
        .add(Objects.requireNonNull(config.driverJavaOptions))
        .add("-cp")
        .add(Objects.requireNonNull(config.standaloneJarPath))
        .add(Objects.requireNonNull(config.standaloneMainClass))
        .add("--pipelineStep=VERBATIM_TO_INTERPRETED");

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

    joiner.add("--conf spark.default.parallelism=" + sparkParallelism)
        .add("--conf spark.executor.memoryOverhead=" + config.sparkMemoryOverhead)
        .add("--class " + Objects.requireNonNull(config.distributedMainClass))
        .add("--master yarn")
        .add("--deploy-mode " + Objects.requireNonNull(config.deployMode))
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

    String interpretationTypes = String.join(",", message.getInterpretTypes());

    // Common properties
    command.add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
        .add("--attempt=" + message.getAttempt())
        .add("--interpretationTypes=" + Objects.requireNonNull(interpretationTypes))
        .add("--runner=SparkRunner")
        .add("--targetPath=" + Objects.requireNonNull(config.repositoryPath))
        .add("--metaFileName=" + Objects.requireNonNull(config.metaFileName))
        .add("--inputPath=" + Objects.requireNonNull(inputPath))
        .add("--avroCompressionType=" + Objects.requireNonNull(config.avroConfig.compressionType))
        .add("--avroSyncInterval=" + config.avroConfig.syncInterval)
        .add("--hdfsSiteConfig=" + Objects.requireNonNull(config.hdfsSiteConfig))
        .add("--coreSiteConfig=" + Objects.requireNonNull(config.coreSiteConfig))
        .add("--wsProperties=" + Objects.requireNonNull(config.wsConfig));

    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(config.otherUser).ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    LOG.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    BiFunction<String, String, File> createDirfn = (String type, String path) -> {
      try {
        Files.createDirectories(Paths.get(path));
        File file = new File(path + message.getDatasetUuid() + "_" + message.getAttempt() + "_int_" + type + ".log");
        LOG.info("{} file - {}", type, file);
        return file;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };

    // The command side outputs
    Optional.ofNullable(config.processErrorDirectory)
        .ifPresent(x -> builder.redirectError(createDirfn.apply("err", x)));
    Optional.ofNullable(config.processOutputDirectory)
        .ifPresent(x -> builder.redirectOutput(createDirfn.apply("out", x)));

    return builder;
  }

}
