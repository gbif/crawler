package org.gbif.crawler.ws;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(
  basePackages = {
    "org.gbif.ws.server.interceptor",
    "org.gbif.ws.server.aspect",
    "org.gbif.ws.server.filter",
    "org.gbif.ws.server.advice",
    "org.gbif.ws.server.mapper",
    "org.gbif.ws.security",
    "org.gbif.crawler.ws"
  })
@EnableFeignClients
public class CrawlerWsApplication {

  public static void main(String[] args) {
    SpringApplication.run(CrawlerWsApplication.class, args);
  }

}
