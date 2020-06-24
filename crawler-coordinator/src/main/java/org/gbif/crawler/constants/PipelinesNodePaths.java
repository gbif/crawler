/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.constants;

import java.util.function.UnaryOperator;

import javax.annotation.Nullable;

import static java.lang.String.join;

public class PipelinesNodePaths {

  public static final String DELIMITER = "/";
  public static final String PIPELINES_ROOT = "pipelines";
  public static final String OK = "successful";
  public static final String ERR = "error";
  public static final String MQ = "mq";
  public static final String RUNNER_TYPE = "runner";
  public static final String MESSAGE = "message";
  public static final String CLASS_NAME = "className";
  public static final String DONE = "availability";
  public static final String START = "startDate";
  public static final String END = "endDate";
  public static final String SIZE = "size";

  public static class Fn {

    private Fn() {
      // NOP
    }

    public static final UnaryOperator<String> START_DATE = s -> join(DELIMITER, s, START);
    public static final UnaryOperator<String> END_DATE = s -> join(DELIMITER, s, END);

    public static final UnaryOperator<String> RUNNER = s -> join(DELIMITER, s, RUNNER_TYPE);

    public static final UnaryOperator<String> ERROR = s -> join(DELIMITER, s, ERR);
    public static final UnaryOperator<String> ERROR_AVAILABILITY =
        s -> join(DELIMITER, s, ERR, DONE);
    public static final UnaryOperator<String> ERROR_MESSAGE = s -> join(DELIMITER, s, ERR, MESSAGE);

    public static final UnaryOperator<String> SUCCESSFUL = s -> join(DELIMITER, s, OK);
    public static final UnaryOperator<String> SUCCESSFUL_AVAILABILITY =
        s -> join(DELIMITER, s, OK, DONE);
    public static final UnaryOperator<String> SUCCESSFUL_MESSAGE =
        s -> join(DELIMITER, s, OK, MESSAGE);

    public static final UnaryOperator<String> MQ_MESSAGE = s -> join(DELIMITER, s, MQ, MESSAGE);
    public static final UnaryOperator<String> MQ_CLASS_NAME =
        s -> join(DELIMITER, s, MQ, CLASS_NAME);
  }

  private PipelinesNodePaths() {}

  /**
   * Helper method to retrieve the path in ZooKeeper where all information about a specific dataset
   * can be found.
   *
   * @param id to retrieve path for
   * @return crawl info path for dataset
   */
  public static String getPipelinesInfoPath(@Nullable String id) {
    return getPipelinesInfoPath(id, null);
  }

  /**
   * Helper method to retrieve a path under the {@link #PIPELINES_ROOT} node.
   *
   * @param id of the dataset to get the path for
   * @param path if null we retrieve the path of the {@link #PIPELINES_ROOT} node itself otherwise
   *     we append this path
   * @return ZK path to crawl info
   */
  public static String getPipelinesInfoPath(@Nullable String id, @Nullable String path) {
    String resultPath = DELIMITER + PIPELINES_ROOT;
    if (id != null) {
      resultPath += DELIMITER + id;
    }
    if (path != null) {
      resultPath += DELIMITER + path;
    }

    return resultPath;
  }
}
