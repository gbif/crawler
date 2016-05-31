package org.gbif.util.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to recursively delete the contents of a node in ZooKeeper.  The passed in node is not deleted - only the
 * nodes below it.
 */
public class ZookeeperCleanup {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCleanup.class);

  private ZookeeperCleanup() {
  }

  /**
   * First and only arg needs to be the node whose content should be deleted, e.g. "/dev_crawler".
   */
  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    if (args.length != 2) {
      LOG.error("Usage: ZookeeperCleanup <node name> <zk path e.g. c1n8.gbif.org:2181,c1n9.gbif.org:2181,c1n10.gbif.org:2181>");
      System.exit(1);
    }
    LOG.debug("ZookeeperCleanup starting");
    ZookeeperCleaner zkCleaner = new ZookeeperCleaner(args[1]);
    zkCleaner.clean(args[0], true);
    LOG.debug("ZookeeperCleanup finished");
  }
}
