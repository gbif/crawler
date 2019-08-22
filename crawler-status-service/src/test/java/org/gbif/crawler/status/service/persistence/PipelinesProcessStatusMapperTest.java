package org.gbif.crawler.status.service.persistence;

import org.gbif.crawler.status.service.guice.CrawlerStatusServiceModule;
import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import com.google.inject.Guice;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.*;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.gbif.crawler.status.service.guice.CrawlerStatusServiceModule.PROPS_PREFIX;

public class PipelinesProcessStatusMapperTest {

  private static PipelinesProcessMapper pipelinesProcessMapper;

  @ClassRule
  public static PostgreSQLContainer postgresDb = new PostgreSQLContainer();

  @BeforeClass
  public static void setup() {
    postgresDb.start();
    runLiquibase();
    pipelinesProcessMapper = Guice.createInjector(new CrawlerStatusServiceModule(createDbProperties()))
      .getInstance(PipelinesProcessMapper.class);
  }

  @AfterClass
  public static void tearDown() {
    postgresDb.stop();
  }

  @Test
  public void createPipelinesProcessTest() {
    PipelinesProcessStatus process = new PipelinesProcessStatus();
    process.setDatasetKey(UUID.randomUUID());
    process.setAttempt(1);
    process.setDatasetTitle("title");

    pipelinesProcessMapper.create(process);

    Assert.assertNotNull(process.getId());
  }

  private static Properties createDbProperties() {
    Properties dbProperties = new Properties();
    dbProperties.setProperty(PROPS_PREFIX + "db.dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
    dbProperties.setProperty(PROPS_PREFIX + "db.dataSource.serverName", "localhost:" + postgresDb.getFirstMappedPort());
    dbProperties.setProperty(PROPS_PREFIX + "db.dataSource.databaseName", "test");
    dbProperties.setProperty(PROPS_PREFIX + "db.dataSource.user", postgresDb.getUsername());
    dbProperties.setProperty(PROPS_PREFIX + "db.dataSource.password", postgresDb.getPassword());
    dbProperties.setProperty(PROPS_PREFIX + "db.maximumPoolSize", "4");
    dbProperties.setProperty(PROPS_PREFIX + "db.connectionTimeout", "3000");
    dbProperties.setProperty(PROPS_PREFIX + "db.minimumIdle", "1");
    dbProperties.setProperty(PROPS_PREFIX + "db.idleTimeout", "6000");

    return dbProperties;
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
