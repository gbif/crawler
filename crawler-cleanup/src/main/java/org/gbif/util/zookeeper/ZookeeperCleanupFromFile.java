/*
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
package org.gbif.util.zookeeper;

import org.gbif.util.HueCsvReader;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperCleanupFromFile {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCleanupFromFile.class);
  private static final String PROD = "prod";
  private static final String UAT = "uat";
  private static final String DEV = "dev";
  private static final String DEV2 = "dev2";
  private static final String UAT2 = "uat2";
  private static final String PROD_PATH = "/prod_crawler/crawls/";
  private static final String UAT_PATH = "/uat_crawler/crawls/";
  private static final String UAT2_PATH = "/uat2_crawler/crawls/";
  private static final String DEV_PATH = "/dev_crawler/crawls/";
  private static final String DEV2_PATH = "/dev2_crawler/crawls/";
  private static final String PROD_ZK =
      "c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181";
  private static final String UAT_ZK =
      "gbif-zookeeper-server-default-0.gbif-zookeeper-server-default.uat.svc.cluster.local:2282,gbif-zookeeper-server-default-1.gbif-zookeeper-server-default.uat.svc.cluster.local:2282,gbif-zookeeper-server-default-2.gbif-zookeeper-server-default.uat.svc.cluster.local:2282,gbif-zookeeper-server-default-3.gbif-zookeeper-server-default.uat.svc.cluster.local:2282,gbif-zookeeper-server-default-4.gbif-zookeeper-server-default.uat.svc.cluster.local:2282";
  private static final String DEV_ZK =
      "c3zk1.gbif-dev.org:2181,c3zk2.gbif-dev.org:2181,c3zk3.gbif-dev.org:2181";
  private static final String DEV2_ZK =
      "gbif-zookeeper-server-default-0.gbif-zookeeper-server-default.gbif-develop.svc.cluster.local:2282,gbif-zookeeper-server-default-1.gbif-zookeeper-server-default.gbif-develop.svc.cluster.local:2282,gbif-zookeeper-server-default-2.gbif-zookeeper-server-default.gbif-develop.svc.cluster.local:2282";
  private static final String UAT2_ZK =
      "c8n1.gbif.org:31930,c8n2.gbif.org:31930,c8n3.gbif.org:31930,c8n5.gbif.org:31930,c8n9.gbif.org:31930";

  private ZookeeperCleanupFromFile() {}

  /** Delete crawls specified in file from given environment */
  public static void main(String[] args) throws IOException, InterruptedException {
    LOG.debug("ZookeeperCleanupFromFile starting");
    if (args.length != 2) {
      LOG.error("Usage: ZookeeperCleanupFromFile <filename> <environment: prod, uat, dev or dev2>");
      System.exit(1);
    }

    String path;
    String zkPath;
    switch (args[1]) {
      case PROD:
        path = PROD_PATH;
        zkPath = PROD_ZK;
        break;
      case UAT:
        path = UAT_PATH;
        zkPath = UAT_ZK;
        break;
      case DEV:
        path = DEV_PATH;
        zkPath = DEV_ZK;
        break;
      case DEV2:
        path = DEV2_PATH;
        zkPath = DEV2_ZK;
        break;
      case UAT2:
        path = UAT2_PATH;
        zkPath = UAT2_ZK;
        break;
      default:
        throw new IllegalArgumentException("Environment must be one of: prod, uat, dev, dev2 or uat2");
    }

    List<String> keys = HueCsvReader.readKeys(args[0]);
    ZookeeperCleaner zkCleaner = new ZookeeperCleaner(zkPath);
    for (String key : keys) {
      LOG.debug("Deleting [{}{}]", path, key);
      zkCleaner.clean(path + key, false);
    }

    LOG.debug("ZookeeperCleanupFromFile finished");
  }
}
