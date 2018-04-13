package org.gbif.crawler.pipelines.service.interpretation;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessRunnerBuilder {

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
  private File error;
  private File output;
  private String hdfsConfigPath;
  private String defaultTargetDirectory;
  private String inputFile;

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

  public ProcessRunnerBuilder error(File error) {
    this.error = error;
    return this;
  }

  public ProcessRunnerBuilder output(File output) {
    this.output = output;
    return this;
  }

  public ProcessRunnerBuilder hdfsConfigPath(String hdfsConfigPath) {
    this.hdfsConfigPath = hdfsConfigPath;
    return this;
  }

  public ProcessRunnerBuilder defaultTargetDirectory(String defaultTargetDirectory) {
    this.defaultTargetDirectory = defaultTargetDirectory;
    return this;
  }

  public ProcessRunnerBuilder inputFile(String inputFile) {
    this.inputFile = inputFile;
    return this;
  }

  public static ProcessRunnerBuilder create() {
    return new ProcessRunnerBuilder();
  }

  public static ProcessRunnerBuilder create(InterpretationConfiguration config) {
    return ProcessRunnerBuilder.create()
      .user(config.user)
      .sparkParallelism(config.sparkParallelism)
      .directParallelism(config.directParallelism)
      .memoryOverhead(config.memoryOverhead)
      .mainClass(config.mainClass)
      .executorMemory(config.executorMemory)
      .executorCores(config.executorCores)
      .executorNumbers(config.executorNumbers)
      .jarFullPath(config.jarFullPath)
      .hdfsConfigPath(config.hdfsConfigPath)
      .error(config.error)
      .output(config.output);

  }

  public Process start() throws IOException {
    if (RunnerEnum.DIRECT == runner) {
      return startDirect();
    }
    if (RunnerEnum.SPARK == runner) {
      return startSpark();
    }
    throw new IllegalArgumentException("Wrong runner type - " + runner);
  }

  private Process startDirect() throws IOException {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("java -cp")
      .add(jarFullPath)
      .add(mainClass)
      .add("--targetParallelism=" + directParallelism);

    return start(joiner);
  }

  private Process startSpark() throws IOException {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark-submit")
      .add("--conf spark.default.parallelism=" + sparkParallelism)
      .add("--conf spark.yarn.executor.memoryOverhead=" + memoryOverhead)
      .add("--class " + mainClass)
      .add("--master yarn")
      .add("--executor-memory " + executorMemory)
      .add("--executor-cores " + executorCores)
      .add("--num-executors " + executorNumbers)
      .add(jarFullPath);

    return start(joiner);
  }

  private Process start(StringJoiner command) throws IOException {
    command.add("--datasetId=" + datasetId)
      .add("--interpretationTypes=" + interpretationTypes)
      .add("--runner=" + runner.getName())
      .add("--defaultTargetDirectory=" + defaultTargetDirectory)
      .add("--inputFile=" + inputFile);

    Optional.ofNullable(hdfsConfigPath).ifPresent(x -> command.add("--hdfsConfiguration=" + x));

    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(user).ifPresent(x -> joiner.add("sudo -u " + x));

    joiner.merge(command);

    String result = joiner.toString();
    LOG.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);
    Optional.ofNullable(error).ifPresent(builder::redirectError);
    Optional.ofNullable(output).ifPresent(builder::redirectOutput);
    return builder.start();
  }

}
