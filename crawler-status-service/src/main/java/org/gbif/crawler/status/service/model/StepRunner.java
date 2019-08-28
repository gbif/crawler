package org.gbif.crawler.status.service.model;

/**
 * General runners, STANDALONE - run an app using local resources, DISTRIBUTED - run an app using
 * YARN cluster.
 */
public enum StepRunner {
  STANDALONE,
  DISTRIBUTED,
  UNKNOWN
}
