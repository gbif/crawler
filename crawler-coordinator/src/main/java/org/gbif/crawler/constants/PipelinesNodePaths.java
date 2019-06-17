package org.gbif.crawler.constants;

import java.util.Set;
import java.util.function.UnaryOperator;

import com.google.common.collect.Sets;
import javax.annotation.Nullable;

import static java.lang.String.join;

import static com.google.common.base.Preconditions.checkNotNull;

public class PipelinesNodePaths {

  private static final String DELIMITER = "/";

  private static final String OK = "successful";
  private static final String ERR = "error";
  private static final String START = "startDate";
  private static final String END = "endDate";
  private static final String MESSAGE = "message";
  private static final String DONE = "availability";

  public static final String PIPELINES_ROOT = "pipelines";

  public static final String SIZE = "size";

  public static final String DWCA_TO_VERBATIM = "dwcaToVerbatim";
  public static final String XML_TO_VERBATIM = "xmlToVerbatim";
  public static final String ABCD_TO_VERBATIM = "abcdToVerbatim";
  public static final String VERBATIM_TO_INTERPRETED = "verbatimToInterpreted";
  public static final String INTERPRETED_TO_INDEX = "interpretedToIndex";
  public static final String HIVE_VIEW = "hiveView";

  public static final Set<String> ALL_STEPS =
    Sets.newHashSet(XML_TO_VERBATIM, ABCD_TO_VERBATIM, DWCA_TO_VERBATIM, VERBATIM_TO_INTERPRETED, INTERPRETED_TO_INDEX, HIVE_VIEW);

  public static class Fn {

    private Fn() {
      // NOP
    }

    public static final UnaryOperator<String> START_DATE = s -> join(DELIMITER, s, START);
    public static final UnaryOperator<String> END_DATE = s -> join(DELIMITER, s, END);

    public static final UnaryOperator<String> ERROR = s -> s + join(DELIMITER, s, ERR);
    public static final UnaryOperator<String> ERROR_AVAILABILITY = s -> s + join(DELIMITER, s, ERR, DONE);
    public static final UnaryOperator<String> ERROR_MESSAGE = s -> s + join(DELIMITER, s, ERR, MESSAGE);

    public static final UnaryOperator<String> SUCCESSFUL = s -> s + join(DELIMITER, s, OK);
    public static final UnaryOperator<String> SUCCESSFUL_AVAILABILITY = s -> s + join(DELIMITER, s, OK, DONE);
    public static final UnaryOperator<String> SUCCESSFUL_MESSAGE = s -> s + join(DELIMITER, s, OK, MESSAGE);
  }

  private PipelinesNodePaths() {
  }

  /**
   * Helper method to retrieve the path in ZooKeeper where all information about a specific dataset can be found.
   *
   * @param id to retrieve path for
   *
   * @return crawl info path for dataset
   */
  public static String getPipelinesInfoPath(String id) {
    return getPipelinesInfoPath(id, null);
  }

  /**
   * Helper method to retrieve a path under the {@link #PIPELINES_ROOT} node.
   *
   * @param id   of the dataset to get the path for
   * @param path if null we retrieve the path of the {@link #PIPELINES_ROOT} node itself otherwise we append this path
   *
   * @return ZK path to crawl info
   */
  public static String getPipelinesInfoPath(String id, @Nullable String path) {
    checkNotNull(id, "uuid can't be null");
    String resultPath = DELIMITER + PIPELINES_ROOT + DELIMITER + id;
    if (path != null) {
      resultPath += DELIMITER + path;
    }

    return resultPath;
  }

}
