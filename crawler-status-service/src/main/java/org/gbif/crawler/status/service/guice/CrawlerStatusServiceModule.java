package org.gbif.crawler.status.service.guice;

import org.gbif.crawler.status.service.PipelinesCoordinatorService;
import org.gbif.crawler.status.service.impl.PipelinesCoordinatorServiceImpl;
import org.gbif.crawler.status.service.persistence.PipelinesProcessMapper;
import org.gbif.crawler.status.service.persistence.handlers.MetricInfoTypeHandler;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;
import org.gbif.mybatis.guice.MyBatisModule;
import org.gbif.mybatis.type.UuidTypeHandler;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

import com.google.inject.Scopes;
import org.apache.ibatis.type.LocalDateTimeTypeHandler;

/**
 * Guice module to set up the injection of the crawler-status-service module.
 */
public class CrawlerStatusServiceModule extends PrivateServiceModule {

  public static final String PROPS_PREFIX = "crawler.status.";
  public static final String DB_PROPS_PREFIX = "db.";

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
    install(new CrawlerStatusMyBatisModule(PropertiesUtil.filterProperties(getProperties(), DB_PROPS_PREFIX)));
    bind(PipelinesCoordinatorService.class).to(PipelinesCoordinatorServiceImpl.class).in(Scopes.SINGLETON);
    expose(PipelinesProcessMapper.class);
    expose(PipelinesCoordinatorService.class);
  }

  /**
   * Module that sets up the DB.
   */
  private static class CrawlerStatusMyBatisModule extends MyBatisModule {

    public CrawlerStatusMyBatisModule(Properties properties) {
      super(properties);
    }

    @Override
    protected void bindMappers() {
      // mappers
      addMapperClass(PipelinesProcessMapper.class);

      // alias
      addAlias("PipelinesProcess").to(PipelinesProcessStatus.class);
      addAlias("Step").to(PipelinesProcessStatus.PipelinesStep.class);
      addAlias("MetricInfoTypeHandler").to(MetricInfoTypeHandler.class);
      addAlias("UuidTypeHandler").to(UuidTypeHandler.class);
      addAlias("LocalDateTimeTypeHandler").to(LocalDateTimeTypeHandler.class);
    }

    @Override
    protected void bindTypeHandlers() {
      handleType(UUID.class).with(UuidTypeHandler.class);
      handleType(LocalDateTime.class).with(LocalDateTimeTypeHandler.class);
      addTypeHandlerClass(MetricInfoTypeHandler.class);
    }

    @Override
    protected void bindManagers() {
      failFast(true);
    }
  }
}
