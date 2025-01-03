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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to recursively delete the contents of a node in ZooKeeper. The passed in node is not
 * deleted - only the nodes below it.
 */
public class ZookeeperCleanup {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCleanup.class);

  private ZookeeperCleanup() {}

  /**
   * First and only arg needs to be the node whose content should be deleted, e.g. "/dev_crawler".
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 2) {
      LOG.error(
          "Usage: ZookeeperCleanup <node name> <zk path e.g. c1n8.gbif.org:2181,c1n9.gbif.org:2181,c1n10.gbif.org:2181>");
      System.exit(1);
    }
    LOG.debug("ZookeeperCleanup starting");
    ZookeeperCleaner zkCleaner = new ZookeeperCleaner(args[1]);
    zkCleaner.clean(args[0], true);
    LOG.debug("ZookeeperCleanup finished");
  }
}
