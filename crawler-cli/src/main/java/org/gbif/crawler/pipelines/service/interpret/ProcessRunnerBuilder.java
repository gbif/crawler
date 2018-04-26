package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.crawler.pipelines.config.InterpreterConfiguration;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to build an instance of ProcessBuilder for direct or spark command
 */
public class ProcessRunnerBuilder {

  public enum RunnerEnum {

    DIRECT("DirectRunner"), SPARK("SparkRunner");

    String name;

    RunnerEnum(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(ProcessRunnerBuilder.class);

  private static final String DELIMITER = " ";

  private String user;
  private RunnerEnum runner;
  private String datasetId;
  private int attempt;
  private Integer sparkParallelism;
  private Integer sparkMemoryOverhead;
  private String sparkExecutorMemory;
  private Integer sparkExecutorCores;
  private Integer sparkExecutorNumbers;
  private String sparkDriverMemory;
  private Integer directParallelism;
  private String directStackSize;
  private String directHeapSize;
  private String jarFullPath;
  private String mainClass;
  private String interpretationTypes;
  private String hdfsSiteConfig;
  private String coreSiteConfig;
  private String targetDirectory;
  private String inputFile;
  private String avroCompressionType;
  private Integer avroSyncInterval;
  private String wsConfig;
  private String redirectErrorFile;
  private String redirectOutputFile;

  public ProcessRunnerBuilder user(String user) {
    this.user = user;
    return this;
  }

  public ProcessRunnerBuilder runner(RunnerEnum runner) {
    this.runner = runner;
    return this;
  }

  public ProcessRunnerBuilder sparkParallelism(Integer sparkParallelism) {
    this.sparkParallelism = sparkParallelism;
    return this;
  }

  public ProcessRunnerBuilder sparkMemoryOverhead(Integer sparkMemoryOverhead) {
    this.sparkMemoryOverhead = sparkMemoryOverhead;
    return this;
  }

  public ProcessRunnerBuilder sparkExecutorMemory(String sparkExecutorMemory) {
    this.sparkExecutorMemory = sparkExecutorMemory;
    return this;
  }

  public ProcessRunnerBuilder sparkExecutorCores(Integer sparkExecutorCores) {
    this.sparkExecutorCores = sparkExecutorCores;
    return this;
  }

  public ProcessRunnerBuilder sparkExecutorNumbers(Integer sparkExecutorNumbers) {
    this.sparkExecutorNumbers = sparkExecutorNumbers;
    return this;
  }

  public ProcessRunnerBuilder sparkDriverMemory(String sparkDriverMemory) {
    this.sparkDriverMemory = sparkDriverMemory;
    return this;
  }

  public ProcessRunnerBuilder directParallelism(Integer directParallelism) {
    this.directParallelism = directParallelism;
    return this;
  }

  public ProcessRunnerBuilder directStackSize(String directStackSize) {
    this.directStackSize = directStackSize;
    return this;
  }

  public ProcessRunnerBuilder directHeapSize(String directHeapSize) {
    this.directHeapSize = directHeapSize;
    return this;
  }

  public ProcessRunnerBuilder jarFullPath(String jarFullPath) {
    this.jarFullPath = jarFullPath;
    return this;
  }

  public ProcessRunnerBuilder mainClass(String mainClass) {
    this.mainClass = mainClass;
    return this;
  }

  public ProcessRunnerBuilder datasetId(String datasetId) {
    this.datasetId = datasetId;
    return this;
  }

  public ProcessRunnerBuilder attempt(int attempt) {
    this.attempt = attempt;
    return this;
  }

  public ProcessRunnerBuilder interpretationTypes(String... interpretationTypes) {
    this.interpretationTypes = Arrays.stream(interpretationTypes).collect(Collectors.joining(","));
    return this;
  }

  public ProcessRunnerBuilder redirectErrorFile(String redirectErrorFile) {
    this.redirectErrorFile = redirectErrorFile;
    return this;
  }

  public ProcessRunnerBuilder redirectOutputFile(String redirectOutputFile) {
    this.redirectOutputFile = redirectOutputFile;
    return this;
  }

  public ProcessRunnerBuilder hdfsSiteConfig(String hdfsSiteConfig) {
    this.hdfsSiteConfig = hdfsSiteConfig;
    return this;
  }

  public ProcessRunnerBuilder coreSiteConfig(String coreSiteConfig) {
    this.coreSiteConfig = coreSiteConfig;
    return this;
  }

  public ProcessRunnerBuilder targetDirectory(String targetDirectory) {
    this.targetDirectory = targetDirectory;
    return this;
  }

  public ProcessRunnerBuilder inputFile(String inputFile) {
    this.inputFile = inputFile;
    return this;
  }

  public ProcessRunnerBuilder avroCompressionType(String avroCompressionType) {
    this.avroCompressionType = avroCompressionType;
    return this;
  }

  public ProcessRunnerBuilder avroSyncInterval(Integer avroSyncInterval) {
    this.avroSyncInterval = avroSyncInterval;
    return this;
  }

  public ProcessRunnerBuilder wsConfig(String wsConfig) {
    this.wsConfig = wsConfig;
    return this;
  }

  public static ProcessRunnerBuilder create() {
    return new ProcessRunnerBuilder();
  }

  public static ProcessRunnerBuilder create(InterpreterConfiguration config) {
    return ProcessRunnerBuilder.create()
      .user(config.otherUser)
      .directParallelism(config.directParallelism)
      .directStackSize(config.directStackSize)
      .directHeapSize(config.directHeapSize)
      .sparkParallelism(config.sparkParallelism)
      .sparkMemoryOverhead(config.sparkMemoryOverhead)
      .sparkExecutorMemory(config.sparkExecutorMemory)
      .sparkExecutorCores(config.sparkExecutorCores)
      .sparkExecutorNumbers(config.sparkExecutorNumbers)
      .sparkDriverMemory(config.sparkDriverMemory)
      .jarFullPath(config.jarFullPath)
      .mainClass(config.mainClass)
      .hdfsSiteConfig(config.hdfsSiteConfig)
      .coreSiteConfig(config.coreSiteConfig)
      .targetDirectory(config.targetDirectory)
      .avroCompressionType(config.avroConfig.compressionType)
      .avroSyncInterval(config.avroConfig.syncInterval)
      .wsConfig(config.wsConfig);
  }

  public ProcessBuilder build() {
    if (RunnerEnum.DIRECT == runner) {
      return buildDirect();
    }
    if (RunnerEnum.SPARK == runner) {
      return buildSpark();
    }
    throw new IllegalArgumentException("Wrong runner type - " + runner);
  }

  /**
   * Builds ProcessBuilder to process direct command
   */
  private ProcessBuilder buildDirect() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("java")
      .add("-Xms" + Objects.requireNonNull(directStackSize))
      .add("-Xmx" + Objects.requireNonNull(directHeapSize))
      .add("-cp")
      .add(Objects.requireNonNull(jarFullPath))
      .add(Objects.requireNonNull(mainClass))
      .add("--wsProperties=" + Objects.requireNonNull(wsConfig));

    Optional.ofNullable(directParallelism).ifPresent(x -> joiner.add("--targetParallelism=" + x));

    return buildCommon(joiner);
  }

  /**
   * Builds ProcessBuilder to process spark command
   */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark-submit")
      .add("--properties-file " + Objects.requireNonNull(wsConfig))
      .add("--conf spark.default.parallelism=" + Objects.requireNonNull(sparkParallelism))
      .add("--conf spark.yarn.executor.memoryOverhead=" + Objects.requireNonNull(sparkMemoryOverhead))
      .add("--class " + Objects.requireNonNull(mainClass))
      .add("--master yarn")
      .add("--deploy-mode cluster")
      .add("--executor-memory " + Objects.requireNonNull(sparkExecutorMemory))
      .add("--executor-cores " + Objects.requireNonNull(sparkExecutorCores))
      .add("--num-executors " + Objects.requireNonNull(sparkExecutorNumbers))
      .add("--driver-memory " + Objects.requireNonNull(sparkDriverMemory))
      .add(Objects.requireNonNull(jarFullPath))
      .add("--wsProperties=" + new File(Objects.requireNonNull(wsConfig)).getName());

    return buildCommon(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline options
   */
  private ProcessBuilder buildCommon(StringJoiner command) {
    // Common properties
    command.add("--datasetId=" + Objects.requireNonNull(datasetId))
      .add("--attempt=" + attempt)
      .add("--interpretationTypes=" + Objects.requireNonNull(interpretationTypes))
      .add("--runner=" + Objects.requireNonNull(runner).getName())
      .add("--defaultTargetDirectory=" + Objects.requireNonNull(targetDirectory))
      .add("--inputFile=" + Objects.requireNonNull(inputFile))
      .add("--avroCompressionType=" + Objects.requireNonNull(avroCompressionType))
      .add("--avroSyncInterval=" + Objects.requireNonNull(avroSyncInterval))
      .add("--hdfsSiteConfig=" + Objects.requireNonNull(hdfsSiteConfig))
      .add("--coreSiteConfig=" + Objects.requireNonNull(coreSiteConfig));

    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(user).ifPresent(x -> joiner.add("sudo -u " + x));
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
