package org.gbif.crawler.common;

import org.gbif.crawler.constants.CrawlerNodePaths;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.CrawlerNodePaths.getCrawlInfoPath;

public class ZookeeperUtils {

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperUtils.class);

  public static void createOrUpdate(CuratorFramework curator, String crawlPath, byte[] data) {
    try {
      Stat stat = curator.checkExists().forPath(crawlPath);
      if (stat == null) {
        curator.create().creatingParentsIfNeeded().forPath(crawlPath, data);
      } else {
        curator.setData().forPath(crawlPath, data);
      }
    } catch (Exception e1) {
      LOG.error("Exception while updating ZooKeeper", e1);
    }
  }

  public static void createOrUpdate(CuratorFramework curator, UUID datasetKey, String subPath, byte[] data) {
    createOrUpdate(curator, CrawlerNodePaths.getCrawlInfoPath(datasetKey, subPath), data);
  }

  public static void createOrUpdate(CuratorFramework curator, UUID datasetKey, String subPath, Enum<?> data) {
    createOrUpdate(curator, datasetKey, subPath, data.name().getBytes(Charsets.UTF_8));
  }

  /**
   * Updates a node in ZooKeeper saving the current date in time in there.
   *
   * @param datasetKey designates the first bit of the path to update
   * @param path       the path to update within the dataset node
   */
  public static void updateDate(CuratorFramework curator, UUID datasetKey, String path) {
    String crawlPath = getCrawlInfoPath(datasetKey, path);
    Date date = new Date();

    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    byte[] data = dateFormat.format(date).getBytes(Charsets.UTF_8);
    createOrUpdate(curator, crawlPath, data);
  }

  public static DistributedAtomicLong getCounter(CuratorFramework curator, UUID datasetKey, String path) {
    return new DistributedAtomicLong(curator, getCrawlInfoPath(datasetKey, path), new RetryNTimes(5, 1000));
  }

  public static void updateCounter(CuratorFramework curator, UUID datasetKey, String path, long value) {
    DistributedAtomicLong dal = getCounter(curator, datasetKey, path);
    try {
      AtomicValue<Long> atom = dal.trySet(value);
      // we must check if the operation actually succeeded
      // see https://github.com/Netflix/curator/wiki/Distributed-Atomic-Long
      if (!atom.succeeded()) {
        LOG.error("Failed to update counter {} for dataset {}", path, datasetKey);
      }
    } catch (Exception e) {
      LOG.error("Failed to update counter {} for dataset {}", path, datasetKey, e);
    }
  }

}
