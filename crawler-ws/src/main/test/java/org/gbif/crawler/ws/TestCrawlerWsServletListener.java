package org.gbif.crawler.ws;

import org.gbif.ws.app.ConfUtils;
import org.gbif.ws.server.guice.GbifServletListener;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.google.inject.Module;

public class TestCrawlerWsServletListener extends GbifServletListener {

  private static final String APPLICATION_PROPERTIES = "crawler-test.properties";

  public TestCrawlerWsServletListener() throws IOException {
    super(ConfUtils.getAppConfFile(APPLICATION_PROPERTIES), "org.gbif.crawler.ws", false);
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    return null;
  }
}
