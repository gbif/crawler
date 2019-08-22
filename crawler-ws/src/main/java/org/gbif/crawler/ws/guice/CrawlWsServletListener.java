package org.gbif.crawler.ws.guice;

import org.gbif.ws.app.ConfUtils;
import org.gbif.ws.server.guice.GbifServletListener;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletContextEvent;

import com.google.common.collect.Lists;
import com.google.inject.Module;
import org.apache.curator.framework.CuratorFramework;

/**
 * The Crawler-Coordinator WS production module.
 */
public class CrawlWsServletListener extends GbifServletListener {

  public static final String APPLICATION_PROPERTIES = "crawler.properties";

  public CrawlWsServletListener() {
    super(ConfUtils.getAppConfFile(APPLICATION_PROPERTIES), "org.gbif.crawler.ws", false);
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new CrawlerModule(properties));
    return modules;
  }

  @Override
  public void contextDestroyed(ServletContextEvent servletContextEvent) {
    super.contextDestroyed(servletContextEvent);
    ExecutorService es = (ExecutorService) getInjector().getInstance(Executor.class);
    es.shutdown();
    try {
      es.awaitTermination(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }

    CuratorFramework curatorFramework = getInjector().getInstance(CuratorFramework.class);
    curatorFramework.close();
  }
}
