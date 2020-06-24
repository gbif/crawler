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
package org.gbif.util.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperCleaner implements Watcher {

  private final String zkHost;
  //  private static final String ZK_HOST = "localhost:2181";
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCleanup.class);

  private ZooKeeper zk;

  public ZookeeperCleaner(String zkHost) throws IOException {
    this.zkHost = zkHost;
    init();
  }

  private void init() throws IOException {
    LOG.debug("Initiating ZooKeeper connection");
    zk = new ZooKeeper(zkHost, 3000, this);
    // wait for zk to fully connect
    while (!zk.getState().isAlive()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // do nothing
      }
    }
    LOG.debug("ZooKeeper connection created");
  }

  /** Recursively delete the node at the given path from ZooKeeper. */
  public void clean(String path, boolean onlyChildren) throws InterruptedException {
    List<String> paths = null;
    try {
      paths = zk.getChildren(path, false);
    } catch (KeeperException e) {
      LOG.warn("path [{}] has no children, which is strange. Trying for delete anyway.", path);
    }
    recursiveDelete(path, paths);
    if (!onlyChildren) {
      try {
        zk.delete(path, -1);
      } catch (KeeperException e) {
        LOG.warn("Could not delete node at [{}]", path, e);
      }
    }
  }

  public void recursiveDelete(String parentPath, List<String> paths) throws InterruptedException {
    if (paths == null || paths.isEmpty()) {
      return;
    }

    for (String path : paths) {
      try {
        String fullPath = parentPath + '/' + path;
        recursiveDelete(fullPath, zk.getChildren(fullPath, false));
        LOG.debug("Deleting leaf [{}]", fullPath);
        zk.delete(fullPath, -1);
      } catch (KeeperException e) {
        LOG.warn("Could not delete node at [{}]", parentPath + '/' + path, e);
      }
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {}
}
