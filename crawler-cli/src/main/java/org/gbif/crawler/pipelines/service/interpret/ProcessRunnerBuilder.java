package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.crawler.pipelines.config.InterpreterConfiguration;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to build an instance of ProcessBuilder for direct or spark command
 */
class ProcessRunnerBuilder {

  public enum RunnerEnum {
    STANDALONE, DISTRIBUTED
  }

  private static final Logger LOG = LoggerFactory.getLogger(ProcessRunnerBuilder.class);

  private static final String DELIMITER = " ";

  private RunnerEnum runner;
  private InterpreterConfiguration config;
  private ExtendedRecordAvailableMessage message;
  private String redirectErrorFile;
  private String redirectOutputFile;

  ProcessRunnerBuilder config(InterpreterConfiguration config) {
    this.config = Objects.requireNonNull(config);
    return this;
  }

  ProcessRunnerBuilder message(ExtendedRecordAvailableMessage message) {
    this.message = Objects.requireNonNull(message);
    return this;
  }

  ProcessRunnerBuilder runner(RunnerEnum runner) {
    this.runner = Objects.requireNonNull(runner);
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
      .add("--pipelineStep=VERBATIM_TO_INTERPRETED");

    return buildCommon(joiner);
  }

  /**
   * Builds ProcessBuilder to process spark command
   */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark2-submit")
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

    String interpretationTypes = String.join(",", message.getInterpretTypes());

    // Common properties
    command.add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
      .add("--attempt=" + message.getAttempt())
      .add("--interpretationTypes=" + Objects.requireNonNull(interpretationTypes))
      .add("--runner=SparkRunner")
      .add("--targetPath=" + Objects.requireNonNull(config.targetDirectory))
      .add("--inputPath=" + Objects.requireNonNull(message.getInputFile().toString()))
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

    // The command side outputs
    Optional.ofNullable(redirectErrorFile).ifPresent(x -> builder.redirectError(new File(redirectErrorFile)));
    Optional.ofNullable(redirectOutputFile).ifPresent(x -> builder.redirectOutput(new File(redirectOutputFile)));
    return builder;
  }

}
