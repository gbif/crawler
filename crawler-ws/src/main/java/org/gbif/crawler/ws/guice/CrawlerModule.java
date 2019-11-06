package org.gbif.crawler.ws.guice;

import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.crawler.DatasetProcessServiceImpl;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.pipelines.PipelinesRunningProcessService;
import org.gbif.crawler.pipelines.PipelinesRunningProcessServiceImpl;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.*;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import static org.gbif.crawler.constants.PipelinesNodePaths.PIPELINES_ROOT;

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
    bind(PipelinesRunningProcessService.class).to(PipelinesRunningProcessServiceImpl.class).in(Scopes.SINGLETON);
    expose(DatasetProcessService.class);
    expose(PipelinesRunningProcessService.class);
    expose(ExecutorService.class);
    expose(Executor.class);
    expose(CuratorFramework.class);
    expose(RestHighLevelClient.class);
    expose(DatasetService.class);

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
  public ExecutorService provideExecutorService(@Named("crawl.threadCount") int threadCount) {
    checkArgument(threadCount > 0, "threadCount has to be greater than zero");
    return Executors.newFixedThreadPool(threadCount);
  }

  /**
   * Provides an Executor to use for various threading related things. This is shared between all requests.
   */
  @Provides
  @Singleton
  @Inject
  public Executor provideExecutor(ExecutorService executorService) {
    return executorService;
  }

  /**
   * Provides an RestHighLevelClient to use for Elasticsearch queries. This is shared between all requests.
   *
   * @param esUrl url to Elasticsearch instance
   */
  @Provides
  @Singleton
  public RestHighLevelClient provideEsRestClient(@Named("pipelines.esUrl") String esUrl) {
    return new RestHighLevelClient(RestClient.builder(HttpHost.create(esUrl)).build());
  }

  /**
   * Provides an DatasetService to query info about datasets. This is shared between all requests.
   *
   * @param url to registry
   */
  @Provides
  @Singleton
  public DatasetService provideDatasetService(@Named("registry.ws.url") String url) {
    Properties p = new Properties();
    p.setProperty("registry.ws.url", url);
    return Guice.createInjector(new RegistryWsClientModule(p), new AnonymousAuthModule()).getInstance(DatasetService.class);
  }
}
