package org.gbif.crawler.status.service.model;

/** Enum to represent the pipelines step names. */
public enum StepType {
  DWCA_TO_VERBATIM(1),
  XML_TO_VERBATIM(1),
  ABCD_TO_VERBATIM(1),
  VERBATIM_TO_INTERPRETED(2),
  INTERPRETED_TO_INDEX(3),
  HIVE_VIEW(3);

  StepType(int executionOrder) {
    this.executionOrder = executionOrder;
  }

  private int executionOrder;

  public int getExecutionOrder() {
    return executionOrder;
  }
}
