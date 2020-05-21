package org.gbif.crawler.ws.guice;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.ws.client.ClientFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A basic class for setting up the injection to enable metrics.
 */
@Configuration
public class CrawlerModule  {

  /*
  protected void configureService() {
    bind(DatasetProcessService.class).to(DatasetProcessServiceImpl.class).in(Scopes.SINGLETON);

    bind(PipelinesRunningProcessService.class).to(PipelinesRunningProcessServiceImpl.class).asEagerSingleton();
    expose(DatasetProcessService.class);
    expose(PipelinesRunningProcessService.class);
    expose(Executor.class);
    expose(CuratorFramework.class);
    expose(DatasetService.class);

    expose(String.class).annotatedWith(Names.named("overcrawledReportFilePath"));
  }*/

  @Bean
  public CuratorFramework provideZookeperResource(
      @Value("${crawl.server}") String url,
      @Value("${crawl.namespace}") String crawlNamespace,
      @Value("${crawl.server.retryAttempts}") Integer retryAttempts,
      @Value("${crawl.server.retryDelayMs}") Integer retryWait
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
  public Executor provideExecutor(@Value("${crawl.threadCount}") int threadCount) {
    checkArgument(threadCount > 0, "threadCount has to be greater than zero");
    return Executors.newFixedThreadPool(threadCount);
  }

  /**
   * Provides an DatasetService to query info about datasets. This is shared between all requests.
   *
   * @param url to registry
   */
  @Bean
  public DatasetService provideDatasetService(@Value("${registry.ws.url}") String url) {
    ClientFactory clientFactory = new ClientFactory(url);
    return clientFactory.newInstance(DatasetClient.class);
  }
}
