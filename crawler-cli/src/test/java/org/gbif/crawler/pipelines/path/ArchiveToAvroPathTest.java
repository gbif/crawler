package org.gbif.crawler.pipelines.path;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.DWCA;
import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.XML;

public class ArchiveToAvroPathTest {

  private static final String XML_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final String DWCA_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";

  private static final String ATTEMPT = "61";

  private static final String DUMMY_URL = "http://some.new.url";

  private static final String DWCA_FOLDER = "dataset/dwca";
  private static final String XML_FOLDER = "dataset/xml";

  private static final String OUTPUT_FOLDER = new File("dataset/export").toPath().toUri().toString();

  @BeforeClass
  public static void init() {
    new File(OUTPUT_FOLDER).mkdir();
  }

  // Test DwCA path

  @Test
  public void testDwcaAllPos() {
    DataSetConfig config = DataSetConfig.getDataSetConfig(DWCA_FOLDER, DWCA_UUID, OUTPUT_FOLDER);
    ArchiveToAvroPath path = PathFactory.create(DWCA).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
    assertCases(DWCA_FOLDER, DWCA_UUID, OUTPUT_FOLDER, null, path);
  }

  @Test(expected = IllegalStateException.class)
  public void testDwcaInvalidInputDirectory() {
    DataSetConfig config = DataSetConfig.getDataSetConfig("G", DWCA_UUID, OUTPUT_FOLDER);
    PathFactory.create(DWCA).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
  }

  @Test
  public void testDwcaInvalidOutputDirectory() {
    String outputDatasetFolderNeg = new File("dataset/dwca/e").toPath().toUri().toString();
    DataSetConfig config = DataSetConfig.getDataSetConfig(DWCA_FOLDER, DWCA_UUID, outputDatasetFolderNeg);
    ArchiveToAvroPath path = PathFactory.create(DWCA).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
    assertCases(DWCA_FOLDER, DWCA_UUID, outputDatasetFolderNeg, null, path);
  }

  @Test(expected = IllegalStateException.class)
  public void testDwcaInvalidDatasetDirectory() {
    DataSetConfig config = DataSetConfig.getDataSetConfig(DWCA_FOLDER, XML_UUID, OUTPUT_FOLDER);
    PathFactory.create(DWCA).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
  }

  // Test XML path

  @Test
  public void testXmlAllPos() {
    DataSetConfig config = DataSetConfig.getDataSetConfig(XML_FOLDER, XML_UUID, OUTPUT_FOLDER);
    ArchiveToAvroPath path = PathFactory.create(XML).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
    assertCases(XML_FOLDER, XML_UUID, OUTPUT_FOLDER, ATTEMPT, path);
  }

  @Test(expected = IllegalStateException.class)
  public void testXmlInvalidInputDirectory() {
    DataSetConfig config = DataSetConfig.getDataSetConfig("G", XML_UUID, OUTPUT_FOLDER);
    PathFactory.create(XML).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
  }

  @Test
  public void testXmlInvalidOutputDirectory() {
    String outputDatasetFolderNeg = new File("dataset/dwca/e").toPath().toUri().toString();
    DataSetConfig config = DataSetConfig.getDataSetConfig(XML_FOLDER, XML_UUID, outputDatasetFolderNeg);
    ArchiveToAvroPath path = PathFactory.create(XML).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
    assertCases(XML_FOLDER, XML_UUID, outputDatasetFolderNeg, ATTEMPT, path);
  }

  @Test(expected = IllegalStateException.class)
  public void testXmlInvalidDatasetDirectory() {
    DataSetConfig config = DataSetConfig.getDataSetConfig(XML_FOLDER, DWCA_UUID, OUTPUT_FOLDER);
    PathFactory.create(XML).from(config.config, config.message.getDatasetUuid(), config.message.getAttempt());
  }

  private void assertCases(String inputFolder, String uuid, String outputFolder, String attempt, ArchiveToAvroPath paths) {
    String expectedInput = inputFolder + File.separator + uuid + (attempt == null ? "" : "/" + attempt);
    Assert.assertEquals(expectedInput, paths.getInputPath().toString());

    outputFolder = outputFolder.endsWith(File.separator) ? outputFolder : outputFolder + File.separator;
    String expected = URI.create(outputFolder + uuid + File.separator + ATTEMPT + File.separator + "verbatim.avro").getPath();
    String result = paths.getOutputPath().toUri().getPath();

    Assert.assertEquals(expected, result);
  }

  private static class DataSetConfig {

    private final ConverterConfiguration config;
    private final DwcaValidationFinishedMessage message;

    static DataSetConfig getDataSetConfig(String inputFolder, String uuid, String outputFolder) {
      return new DataSetConfig(getConfig(inputFolder, outputFolder), getMessage(uuid));
    }

    /**
     * Get ConverterConfiguration based on provided parameters.
     */
    private static ConverterConfiguration getConfig(String archiveRepo, String exportAvroURL) {
      ConverterConfiguration config = new ConverterConfiguration();
      config.archiveRepository = archiveRepo;
      config.extendedRecordRepository = exportAvroURL;
      return config;
    }

    /**
     * Get DwcaValidationFinishedMessage based on provided datasetUUID.
     */
    private static DwcaValidationFinishedMessage getMessage(String datasetUUID) {
      return new DwcaValidationFinishedMessage(UUID.fromString(datasetUUID),
                                               DatasetType.OCCURRENCE,
                                               URI.create(DUMMY_URL),
                                               Integer.parseInt(ATTEMPT),
                                               new DwcaValidationReport(UUID.fromString(datasetUUID), "no reason"));
    }

    private DataSetConfig(ConverterConfiguration config, DwcaValidationFinishedMessage message) {
      this.config = config;
      this.message = message;
    }
  }

}
