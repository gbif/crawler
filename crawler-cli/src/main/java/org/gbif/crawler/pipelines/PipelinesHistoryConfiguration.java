package org.gbif.crawler.pipelines;

import java.util.StringJoiner;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;

/** Class to provide the necessary configuration to use the pipelines history service. */
@SuppressWarnings("PublicField")
public class PipelinesHistoryConfiguration {

  @NotNull
  @Parameter(names = "--history-db-host")
  public String serverName;

  @NotNull
  @Parameter(names = "--history-db-db")
  public String databaseName;

  @NotNull
  @Parameter(names = "--history-db-user")
  public String user;

  @NotNull
  @Parameter(names = "--history-db-password", password = true)
  public String password;

  @Parameter(names = "--history-db-maximumPoolSize")
  public int maximumPoolSize = 3;

  @Parameter(names = "--history-db-connectionTimeout")
  public int connectionTimeout = 3000;

  @Override
  public String toString() {
    return new StringJoiner(", ", PipelinesHistoryConfiguration.class.getSimpleName() + "[", "]")
        .add("serverName='" + serverName + "'")
        .add("databaseName='" + databaseName + "'")
        .add("user='" + user + "'")
        .add("password='" + password + "'")
        .add("maximumPoolSize=" + maximumPoolSize)
        .add("connectionTimeout=" + connectionTimeout)
        .toString();
  }
}
