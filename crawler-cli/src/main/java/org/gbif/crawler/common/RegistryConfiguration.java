package org.gbif.crawler.common;

import org.gbif.cli.PropertyName;
import org.gbif.ws.client.ClientFactory;

import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;
import com.google.common.base.Strings;

/**
 * A configuration class which can be used to get all the details needed to create a writable connection to the
 * GBIF registry.
 */
public class RegistryConfiguration {

  @Parameter(names = "--registry-ws")
  @PropertyName("registry.ws.url")
  @NotNull
  public String wsUrl = "http://api.gbif.org/";

  @Parameter(names = "--registry-user")
  @NotNull
  public String user;

  @Parameter(names = "--registry-password", password = true)
  @NotNull
  public String password;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("registryWsUrl", wsUrl)
        .add("registryUser", user)
        .add("registryPassword", Strings.repeat("*", Strings.nullToEmpty(password).length()))
        .toString();
  }

  /**
   * Convenience method to provide a ws client factory. The factory will be used to create writable registry clients.
   *
   * @return writable client factory
   */
  public ClientFactory newClientFactory() {
    // setup writable registry client
    return new ClientFactory(user, password, wsUrl);
  }

}
