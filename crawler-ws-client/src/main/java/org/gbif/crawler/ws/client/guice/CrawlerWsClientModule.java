package org.gbif.crawler.ws.client.guice;

import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.crawler.ws.client.CrawlerWsClient;
import org.gbif.ws.client.guice.GbifWsClientModule;

import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 * Guice module that exposes the Crawler Service.
 */
public class CrawlerWsClientModule extends GbifWsClientModule {

  public CrawlerWsClientModule(Properties properties) {
    super(properties, CrawlerWsClient.class.getPackage());
  }

  @Override
  protected void configureClient() {
    bind(DatasetProcessService.class).to(CrawlerWsClient.class);
    expose(DatasetProcessService.class);
  }

  /**
   * @return the web resource to the webapp
   */
  @Provides
  @Singleton
  public WebResource provideWebResource(Client client, @Named("crawler.ws.url") String url) {
    return client.resource(url);
  }
}
