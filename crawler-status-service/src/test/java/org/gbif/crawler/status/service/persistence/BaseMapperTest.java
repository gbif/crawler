package org.gbif.crawler.status.service.persistence;

import org.gbif.crawler.status.service.guice.CrawlerStatusServiceModule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.gbif.crawler.status.service.guice.CrawlerStatusServiceModule.DB_PROPS_PREFIX;
import static org.gbif.crawler.status.service.guice.CrawlerStatusServiceModule.PROPS_PREFIX;

/**
 * Base class to tests the myBatis mappers using an embedded postgres database.
 */
public class BaseMapperTest {

  private static final String POSTGRES_IMAGE = "postgres:11.3";

  @ClassRule
  public static PostgreSQLContainer postgresDb = new PostgreSQLContainer(POSTGRES_IMAGE);

  protected static Injector injector;

  @BeforeClass
  public static void setup() {
    postgresDb.start();
    runLiquibase();
    injector = Guice.createInjector(new CrawlerStatusServiceModule(createDbProperties()));
  }

  @AfterClass
  public static void tearDown() {
    postgresDb.stop();
  }

  private static Properties createDbProperties() {
    Properties dbProperties = new Properties();
    dbProperties.setProperty(PROPS_PREFIX + DB_PROPS_PREFIX + "dataSourceClassName",
                             "org.postgresql.ds.PGSimpleDataSource");
    dbProperties.setProperty(PROPS_PREFIX + DB_PROPS_PREFIX + "dataSource.serverName",
                             "localhost:" + postgresDb.getFirstMappedPort());
    dbProperties.setProperty(PROPS_PREFIX + DB_PROPS_PREFIX + "dataSource.databaseName", "test");
    dbProperties.setProperty(PROPS_PREFIX + DB_PROPS_PREFIX + "dataSource.user", postgresDb.getUsername());
    dbProperties.setProperty(PROPS_PREFIX + DB_PROPS_PREFIX + "dataSource.password", postgresDb.getPassword());

    return dbProperties;
  }

  @Before
  public void clearDB() {
    try {
      Class.forName("org.postgresql.Driver");
      try (Connection connection = DriverManager.getConnection(postgresDb.getJdbcUrl(),
                                                               postgresDb.getUsername(),
                                                               postgresDb.getPassword())) {
        connection.prepareStatement("DELETE FROM pipelines_step").executeUpdate();
        connection.prepareStatement("DELETE FROM pipelines_process").executeUpdate();
      }
    } catch (ClassNotFoundException | SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Executes the liquibase master.xml change logs in the context ddl.
   */
  private static void runLiquibase() {
    try {
      Class.forName("org.postgresql.Driver");
      try (Connection connection = DriverManager.getConnection(postgresDb.getJdbcUrl(),
                                                               postgresDb.getUsername(),
                                                               postgresDb.getPassword())) {
        Liquibase liquibase =
          new Liquibase("liquibase/master.xml", new ClassLoaderResourceAccessor(), new JdbcConnection(connection));
        liquibase.dropAll();
        liquibase.update("ddl");
      }
    } catch (ClassNotFoundException | SQLException | LiquibaseException ex) {
      throw new IllegalStateException(ex);
    }
  }

}
