package org.gbif.crawler.ws.client.guice;

import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.crawler.ws.client.CrawlerWsClient;
import org.gbif.ws.client.guice.GbifApplicationAuthModule;
import org.gbif.ws.client.guice.GbifWsClientModule;

import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import static org.gbif.ws.client.guice.GbifApplicationAuthModule.PROPERTY_APP_KEY;
import static org.gbif.ws.client.guice.GbifApplicationAuthModule.PROPERTY_APP_SECRET;

/**
 * Guice module that exposes the Crawler Service.
 */
public class CrawlerWsClientModule extends GbifWsClientModule {

  public CrawlerWsClientModule(Properties properties) {
    super(properties, CrawlerWsClient.class.getPackage());
  }

  @Override
  protected void configureClient() {
    if (getProperties().containsKey(PROPERTY_APP_KEY) && getProperties().containsKey(PROPERTY_APP_SECRET)) {
      install(new GbifApplicationAuthModule(getProperties()));
    }
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
