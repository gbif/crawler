package org.gbif.crawler.ws;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.elasticsearch.ElasticSearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude = {
  ElasticsearchAutoConfiguration.class,
  ElasticSearchRestHealthContributorAutoConfiguration.class,
  RestClientAutoConfiguration.class,
  DataSourceAutoConfiguration.class,
  ManagementWebSecurityAutoConfiguration.class,
  SecurityAutoConfiguration.class
})
@EnableConfigurationProperties
@ComponentScan(
  basePackages = {
    "org.gbif.ws.server.interceptor",
    "org.gbif.ws.server.aspect",
    "org.gbif.ws.server.advice",
    "org.gbif.ws.server.mapper",
    "org.gbif.crawler.ws"
  })
public class CrawlerWsApplication {

  public static void main(String[] args) {
    SpringApplication.run(CrawlerWsApplication.class, args);
  }

}
