package org.gbif.crawler.pipelines.service.interpretation;

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
