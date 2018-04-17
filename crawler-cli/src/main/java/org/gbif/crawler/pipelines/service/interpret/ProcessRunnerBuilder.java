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
  private Integer sparkParallelism;
  private Integer directParallelism;
  private Integer memoryOverhead;
  private String mainClass;
  private String executorMemory;
  private Integer executorCores;
  private Integer executorNumbers;
  private String jarFullPath;
  private String datasetId;
  private String interpretationTypes;
  private String hdfsConfigPath;
  private String targetDirectory;
  private String inputFile;
  private String avroCompressionType;
  private Integer avroSyncInterval;
  private String taxonWsConfig;
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

  public ProcessRunnerBuilder directParallelism(Integer directParallelism) {
    this.directParallelism = directParallelism;
    return this;
  }

  public ProcessRunnerBuilder memoryOverhead(Integer memoryOverhead) {
    this.memoryOverhead = memoryOverhead;
    return this;
  }

  public ProcessRunnerBuilder mainClass(String mainClass) {
    this.mainClass = mainClass;
    return this;
  }

  public ProcessRunnerBuilder executorMemory(String executorMemory) {
    this.executorMemory = executorMemory;
    return this;
  }

  public ProcessRunnerBuilder executorCores(Integer executorCores) {
    this.executorCores = executorCores;
    return this;
  }

  public ProcessRunnerBuilder executorNumbers(Integer executorNumbers) {
    this.executorNumbers = executorNumbers;
    return this;
  }

  public ProcessRunnerBuilder jarFullPath(String jarFullPath) {
    this.jarFullPath = jarFullPath;
    return this;
  }

  public ProcessRunnerBuilder datasetId(String datasetId) {
    this.datasetId = datasetId;
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

  public ProcessRunnerBuilder hdfsConfigPath(String hdfsConfigPath) {
    this.hdfsConfigPath = hdfsConfigPath;
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

  public ProcessRunnerBuilder taxonWsConfig(String taxonWsConfig) {
    this.taxonWsConfig = taxonWsConfig;
    return this;
  }

  public static ProcessRunnerBuilder create() {
    return new ProcessRunnerBuilder();
  }

  public static ProcessRunnerBuilder create(InterpreterConfiguration config) {
    return ProcessRunnerBuilder.create()
      .user(config.otherUser)
      .sparkParallelism(config.sparkParallelism)
      .directParallelism(config.directParallelism)
      .memoryOverhead(config.memoryOverhead)
      .mainClass(config.mainClass)
      .executorMemory(config.executorMemory)
      .executorCores(config.executorCores)
      .executorNumbers(config.executorNumbers)
      .jarFullPath(config.jarFullPath)
      .hdfsConfigPath(config.hdfsSiteConfig)
      .targetDirectory(config.targetDirectory)
      .avroCompressionType(config.avroConfig.compressionType)
      .avroSyncInterval(config.avroConfig.syncInterval)
      .redirectErrorFile(config.proccesErrorFile)
      .redirectOutputFile(config.proccesOutputFile)
      .taxonWsConfig(config.taxonWsConfig);
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
    StringJoiner joiner = new StringJoiner(DELIMITER).add("java -cp")
      .add(Objects.requireNonNull(jarFullPath))
      .add(Objects.requireNonNull(mainClass));

    Optional.ofNullable(directParallelism).ifPresent(x -> joiner.add("--targetParallelism=" + x));

    return build(joiner);
  }

  /**
   * Builds ProcessBuilder to process spark command
   */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark-submit")
      .add("--conf spark.default.parallelism=" + Objects.requireNonNull(sparkParallelism))
      .add("--conf spark.yarn.executor.memoryOverhead=" + Objects.requireNonNull(memoryOverhead))
      .add("--class " + Objects.requireNonNull(mainClass))
      .add("--master yarn")
      .add("--executor-memory " + Objects.requireNonNull(executorMemory))
      .add("--executor-cores " + Objects.requireNonNull(executorCores))
      .add("--num-executors " + Objects.requireNonNull(executorNumbers))
      .add(Objects.requireNonNull(jarFullPath));

    return build(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline options
   */
  private ProcessBuilder build(StringJoiner command) {
    // Common properies
    command.add("--datasetId=" + Objects.requireNonNull(datasetId))
      .add("--interpretationTypes=" + Objects.requireNonNull(interpretationTypes))
      .add("--runner=" + Objects.requireNonNull(runner).getName())
      .add("--defaultTargetDirectory=" + Objects.requireNonNull(targetDirectory))
      .add("--inputFile=" + Objects.requireNonNull(inputFile))
      .add("--avroCompressionType=" + Objects.requireNonNull(avroCompressionType))
      .add("--avroSyncInterval=" + Objects.requireNonNull(avroSyncInterval))
      .add("--wsProperties=" + Objects.requireNonNull(taxonWsConfig));

    // Adds hdfs configuration if it is necessary
    Optional.ofNullable(hdfsConfigPath).ifPresent(x -> command.add("--hdfsConfiguration=" + x));

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
