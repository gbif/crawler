package org.gbif.crawler.pipelines;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

public class PathCacheZk {

  public static void main(String[] args) throws Exception {
    CuratorFramework client = CuratorFrameworkFactory.builder()
      .connectString("c3zk1.gbif-dev.org,c3zk2.gbif-dev.org,c3zk3.gbif-dev.org")
      .namespace("dev_crawler")
      .retryPolicy(new ExponentialBackoffRetry(100, 10))
      .build();
    client.start();

    PathChildrenCache cache = new PathChildrenCache(client, "/pipelines", true);
    cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

    PathChildrenCacheListener listener = (client1, event) -> {

      switch ( event.getType() )
      {
        case CHILD_ADDED:
        {
          System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
          break;
        }

        case CHILD_UPDATED:
        {
          System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
          break;
        }

        case CHILD_REMOVED:
        {
          System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
          break;
        }
      }

    };
    cache.getListenable().addListener(listener);

    boolean close = false;
    while (!close) {}

  }
}
