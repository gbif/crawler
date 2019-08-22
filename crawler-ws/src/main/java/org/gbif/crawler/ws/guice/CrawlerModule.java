package org.gbif.crawler.ws.guice;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.gbif.api.model.registry.Installation;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.DatasetProcessServiceImpl;
import org.gbif.crawler.pipelines.PipelinesProcessService;
import org.gbif.crawler.pipelines.PipelinesProcessServiceImpl;
import org.gbif.registry.metasync.MetadataSynchroniserImpl;
import org.gbif.registry.metasync.api.MetadataSynchroniser;
import org.gbif.registry.metasync.util.HttpClientFactory;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import com.google.inject.Guice;
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
    expose(Executor.class);
    expose(CuratorFramework.class);
    expose(RestHighLevelClient.class);
    expose(DatasetService.class);
    expose(MetadataSynchroniser.class);
    expose(MessagePublisher.class);

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

  /**
   * Provides an MetadataSynchroniser. This is shared between all requests.
   *
   * @param url to registry
   */
  @Provides
  @Singleton
  public MetadataSynchroniser provideMetadataSynchroniser(@Named("registry.ws.url") String url) {
    Properties p = new Properties();
    p.setProperty("registry.ws.url", url);
    InstallationService installationService = Guice.createInjector(new RegistryWsClientModule(p), new AnonymousAuthModule()).getInstance(InstallationService.class);
    return new MetadataSynchroniserImpl(installationService);
  }

  /**
   * Provides an RabbitMQ publisher
   */
  @Provides
  @Singleton
  public MessagePublisher provideMessagePublisher(
      @Named("mq.host") String host,
      @Named("mq.virtualHost") String virtualHost,
      @Named("mq.username") String username,
      @Named("mq.password") String password) {
    MessagingConfiguration configuration = new MessagingConfiguration();
    configuration.host = host;
    configuration.virtualHost = virtualHost;
    configuration.username = username;
    configuration.password = password;
    try {
      return new DefaultMessagePublisher(configuration.getConnectionParameters());
    } catch (IOException e) {
      return null;
    }
  }

}
