package org.gbif.crawler.status.service.guice;

import org.gbif.crawler.status.service.model.PipelineProcess;
import org.gbif.crawler.status.service.model.PipelineStep;
import org.gbif.crawler.status.service.persistence.PipelineProcessMapper;
import org.gbif.crawler.status.service.persistence.handlers.MetricInfoTypeHandler;
import org.gbif.mybatis.guice.MyBatisModule;
import org.gbif.mybatis.type.UuidTypeHandler;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

import org.apache.ibatis.type.LocalDateTimeTypeHandler;

/**
 * Guice module to set up the injection of the crawler-status-service module.
 */
public class CrawlerStatusServiceModule extends PrivateServiceModule {

  private static final String PROPS_PREFIX = "crawler.status.";
  private static final String DB_PROPS_PREFIX = "db.";

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
    expose(PipelineProcessMapper.class);
  }

  /**
   * Module that sets up the DB.
   */
  public static class CrawlerStatusMyBatisModule extends MyBatisModule {

    public CrawlerStatusMyBatisModule(Properties properties) {
      super(properties);
    }

    @Override
    protected void bindMappers() {
      // mappers
      addMapperClass(PipelineProcessMapper.class);

      // alias
      addAlias("PipelineProcess").to(PipelineProcess.class);
      addAlias("Step").to(PipelineStep.class);
      addAlias("MetricInfoTypeHandler").to(MetricInfoTypeHandler.class);
      addAlias("UuidTypeHandler").to(UuidTypeHandler.class);
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
