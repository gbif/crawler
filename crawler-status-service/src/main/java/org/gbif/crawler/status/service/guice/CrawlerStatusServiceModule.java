package org.gbif.crawler.status.service.guice;

import java.util.Properties;

import org.gbif.service.guice.PrivateServiceModule;

public class CrawlerStatusServiceModule extends PrivateServiceModule {

  /**
   * Uses the given properties to configure the service.
   *
   * @param properties to use
   */
  public CrawlerStatusServiceModule(String propertyPrefix, Properties properties) {
    super(propertyPrefix, properties);
  }

  @Override
  protected void configureService() {

  }
}
