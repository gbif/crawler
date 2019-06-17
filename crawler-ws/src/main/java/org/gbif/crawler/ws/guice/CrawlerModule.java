package org.gbif.crawler.ws.guice;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.crawler.DatasetProcessServiceImpl;
import org.gbif.crawler.pipelines.PipelinesProcessService;
import org.gbif.crawler.pipelines.PipelinesProcessServiceImpl;
import org.gbif.service.guice.PrivateServiceModule;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A basic class for setting up the injection to enable metrics.
 */
class CrawlerModule extends PrivateServiceModule {

  public static final String PREFIX = "crawler.";

  CrawlerModule(String propertyPrefix, Properties properties) {
    super(propertyPrefix, properties);
  }

  @Override
  protected void configureService() {
    bind(DatasetProcessService.class).to(DatasetProcessServiceImpl.class).in(Scopes.SINGLETON);
    bind(PipelinesProcessService.class).to(PipelinesProcessServiceImpl.class).in(Scopes.SINGLETON);
    expose(DatasetProcessService.class);
    expose(PipelinesProcessService.class);
    expose(DatasetService.class);
    expose(Executor.class);
    expose(CuratorFramework.class);
    expose(RestHighLevelClient.class);

    expose(String.class).annotatedWith(Names.named("overcrawledReportFilePath"));
    expose(String.class).annotatedWith(Names.named("pipelines.envPrefix"));
  }

  CrawlerModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Provides
  @Singleton
  @Inject
  public CuratorFramework provideZookeperResource(
    @Named("crawl.server") String url,
    @Named("crawl.namespace") String crawlNamespace,
    @Named("crawl.server.retryAttempts") Integer retryAttempts,
    @Named("crawl.server.retryDelayMs") Integer retryWait
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
  @Provides
  @Singleton
  public Executor provideExecutor(@Named("crawl.threadCount") int threadCount) {
    checkArgument(threadCount > 0, "threadCount has to be greater than zero");
    return Executors.newFixedThreadPool(threadCount);
  }

  /**
   * Provides an RestHighLevelClient to use for various threading related things. This is shared between all requests.
   *
   * @param esUrl url to Elasticsearch instance
   */
  @Provides
  @Singleton
  public RestHighLevelClient provideEsRestClient(@Named("pipelines.esUrl") String esUrl){
    return new RestHighLevelClient(RestClient.builder(HttpHost.create(esUrl)).build());
  }

}
