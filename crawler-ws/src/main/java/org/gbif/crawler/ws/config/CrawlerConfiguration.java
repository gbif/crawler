package org.gbif.crawler.ws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.crawler.DatasetProcessServiceImpl;
import org.gbif.crawler.pipelines.PipelinesRunningProcessService;
import org.gbif.crawler.pipelines.PipelinesRunningProcessServiceImpl;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.ws.client.ClientFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A basic class for setting up the injection to enable metrics.
 */
@Configuration
public class CrawlerConfiguration {

  @Bean
  public ObjectMapper crawlerObjectMapper() {
    return JacksonJsonObjectMapperProvider.getObjectMapper();
  }

  @Bean
  public DatasetProcessService datasetProcessService(
      @Qualifier("zookeeperResource") CuratorFramework curator,
      @Qualifier("crawlerObjectMapper") ObjectMapper objectMapper,
      @Qualifier("crawlerExecutor") Executor executor) {
    return new DatasetProcessServiceImpl(curator, objectMapper, executor);
  }

  @Bean
  public PipelinesRunningProcessService pipelinesRunningProcessService(
      @Qualifier("zookeeperResource") CuratorFramework curator,
      DatasetService datasetService) throws Exception {
    return new PipelinesRunningProcessServiceImpl(curator, datasetService);
  }

  @Bean
  public CuratorFramework zookeeperResource(
      @Value("${crawler.crawl.server}") String url,
      @Value("${crawler.crawl.namespace}") String crawlNamespace,
      @Value("${crawler.crawl.server.retryAttempts}") Integer retryAttempts,
      @Value("${crawler.crawl.server.retryDelayMs}") Integer retryWait
  ) {
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(url)
        .namespace(crawlNamespace)
        .retryPolicy(new ExponentialBackoffRetry(retryWait, retryAttempts))
        .build();
    client.start();
    return client;
  }

  /**
   * Provides an Executor to use for various threading related things. This is shared between all requests.
   *
   * @param threadCount number of maximum threads to use
   */
  @Bean
  public Executor crawlerExecutor(@Value("${crawler.crawl.threadCount}") int threadCount) {
    checkArgument(threadCount > 0, "threadCount has to be greater than zero");
    return Executors.newFixedThreadPool(threadCount);
  }

  /**
   * Provides an DatasetService to query info about datasets. This is shared between all requests.
   *
   * @param url to registry
   */
  @Bean
  public DatasetService datasetService(@Value("${crawler.registry.ws.url}") String url) {
    ClientFactory clientFactory = new ClientFactory(url);
    return clientFactory.newInstance(DatasetClient.class);
  }
}
