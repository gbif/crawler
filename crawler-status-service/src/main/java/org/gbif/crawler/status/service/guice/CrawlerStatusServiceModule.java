package org.gbif.crawler.status.service.guice;

import org.gbif.crawler.status.service.persistence.PipelinesProcessStatusMapper;
import org.gbif.crawler.status.service.persistence.handlers.MetricInfoTypeHandler;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;
import org.gbif.mybatis.guice.MyBatisModule;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.util.Properties;

/**
 * Guice module to set up the injection of the crawler-status-service module.
 */
public class CrawlerStatusServiceModule extends PrivateServiceModule {

  private static final String PROPS_PREFIX = "status.";

  /**
   * Uses the given properties to configure the service.
   *
   * @param properties to use
   */
  public CrawlerStatusServiceModule(Properties properties) {
    super(PROPS_PREFIX, properties);
  }

  @Override
  protected void configureService() {
    install(new CrawlerStatusMyBatisModule(getProperties()));
    expose(PipelinesProcessStatusMapper.class);
  }

  /**
   * Module that sets up the DB.
   */
  private static class CrawlerStatusMyBatisModule extends MyBatisModule {

    private static final String DB_PROPS_PREFIX = "db.";

    public CrawlerStatusMyBatisModule(Properties properties) {
      super(PropertiesUtil.filterProperties(properties, DB_PROPS_PREFIX));
    }

    @Override
    protected void bindMappers() {
      // mappers
      addMapperClass(PipelinesProcessStatusMapper.class);

      // alias
      addAlias("PipelinesProcess").to(PipelinesProcessStatus.class);
      addAlias("Step").to(PipelinesProcessStatus.PipelinesStep.class);
      addAlias("MetricInfoTypeHandler").to(MetricInfoTypeHandler.class);
    }

    @Override
    protected void bindTypeHandlers() {
      addTypeHandlerClass(MetricInfoTypeHandler.class);
    }

    @Override
    protected void bindManagers() {
      failFast(true);
    }
  }
}
