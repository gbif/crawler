package org.gbif.crawler.ws.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.gbif.api.service.crawler.DatasetProcessService;
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

  public class CuratorWrapper {

    private CuratorFramework curator;

    CuratorWrapper(
      String url,
      String crawlNamespace,
      Integer retryAttempts,
      Integer retryWait
    ) {
      curator = CuratorFrameworkFactory.builder()
        .connectString(url)
        .namespace(crawlNamespace)
        .retryPolicy(new ExponentialBackoffRetry(retryWait, retryAttempts))
        .build();
      curator.start();
    }

    public CuratorFramework getCurator() {
      return curator;
    }
  }

  @Bean
  public ObjectMapper crawlerObjectMapper() {
    return JacksonJsonObjectMapperProvider.getObjectMapper();
  }

  @Bean
  public DatasetProcessService datasetProcessService(
      @Qualifier("zookeeperResource") CuratorWrapper curatorWrapper,
      @Qualifier("crawlerObjectMapper") ObjectMapper objectMapper,
      @Qualifier("crawlerExecutor") Executor executor) {
    return new DatasetProcessServiceImpl(curatorWrapper.getCurator(), objectMapper, executor);
  }

  @Bean
  public PipelinesRunningProcessService pipelinesRunningProcessService(
      @Qualifier("zookeeperResource") CuratorWrapper curatorWrapper,
      @Value("${crawler.registry.ws.url}") String url) throws Exception {
    ClientFactory clientFactory = new ClientFactory(url);
    return new PipelinesRunningProcessServiceImpl(curatorWrapper.getCurator(), clientFactory.newInstance(DatasetClient.class));
  }

  @Bean("zookeeperResource")
  public CuratorWrapper zookeeperResource(
      @Value("${crawler.crawl.server.zk}") String url,
      @Value("${crawler.crawl.namespace}") String crawlNamespace,
      @Value("${crawler.crawl.server.retryAttempts}") Integer retryAttempts,
      @Value("${crawler.crawl.server.retryDelayMs}") Integer retryWait
  ) {
    return new CuratorWrapper(url, crawlNamespace, retryAttempts, retryWait);
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

}
