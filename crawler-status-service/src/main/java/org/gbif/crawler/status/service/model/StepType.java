package org.gbif.crawler.status.service.model;

/** Enum to represent the pipelines step names. */
public enum StepType {
  DWCA_TO_VERBATIM,
  XML_TO_VERBATIM,
  ABCD_TO_VERBATIM,
  VERBATIM_TO_INTERPRETED,
  INTERPRETED_TO_INDEX,
  HIVE_VIEW
}
