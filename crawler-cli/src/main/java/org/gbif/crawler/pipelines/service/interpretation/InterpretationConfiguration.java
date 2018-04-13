package org.gbif.crawler.pipelines.service.interpretation;

import java.io.File;

public class InterpretationConfiguration {
  String user = "hdfs";
  Integer sparkParallelism = 16;
  Integer directParallelism = 2;
  Integer memoryOverhead = 2048;
  String mainClass = "milestone2.cli.BeamThredControlDemo";
  String executorMemory = "16G";
  Integer executorCores = 2;
  Integer executorNumbers = 3;
  String jarFullPath = "/home/nvolik/labs-1.1-SNAPSHOT-shaded.jar";
  String hdfsConfigPath;
  File error;
  File output;
  String defaultTargetDirectory;
}
