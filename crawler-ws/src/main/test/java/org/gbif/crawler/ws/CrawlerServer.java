package org.gbif.crawler.ws;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.PostgreSQLContainer;

public class CrawlerServer extends ExternalResource {

  static final String POSTGRES_IMAGE = "postgres:11.3";
  public static PostgreSQLContainer postgresDb = new PostgreSQLContainer(POSTGRES_IMAGE);

  @Override
  protected void before() throws Throwable {
    postgresDb.start();
    runLiquibase();
    // TODO: create props for DB

  }

  @Override
  protected void after() {
    postgresDb.stop();
  }



  /** Executes the liquibase master.xml change logs in the context ddl. */
  private static void runLiquibase() {
    try {
      Class.forName("org.postgresql.Driver");
      try (Connection connection =
             DriverManager.getConnection(
               postgresDb.getJdbcUrl(), postgresDb.getUsername(), postgresDb.getPassword())) {
        Liquibase liquibase =
          new Liquibase(
            "liquibase/master.xml",
            new ClassLoaderResourceAccessor(),
            new JdbcConnection(connection));
        liquibase.dropAll();
        liquibase.update("ddl");
      }
    } catch (ClassNotFoundException | SQLException | LiquibaseException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
