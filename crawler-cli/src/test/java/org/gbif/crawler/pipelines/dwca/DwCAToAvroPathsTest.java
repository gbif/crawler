package org.gbif.crawler.pipelines.dwca;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test dwca-to-avro commands Configurations with received message parameter verification
 */
public class DwCAToAvroPathsTest {

  private static final String DATASET_UUID_POS = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DATASET_UUID_NEG = "9bed66b3-4caa-42bb-9c93-71d7ba109dae";

  private static final String DUMMY_URL = "http://some.new.url";

  private static final String INPUT_DATASET_FOLDER_POS = "dataset";
  private static final String INPUT_DATASET_FOLDER_NEG = "d";

  private static final String OUTPUT_DATASET_FOLDER_POS = new File("dataset/export").toPath().toUri().toString();
  private static final String OUTPUT_DATASET_FOLDER_NEG = new File("dataset/e").toPath().toUri().toString();

  @BeforeClass
  public static void init() {
    new File(OUTPUT_DATASET_FOLDER_POS).mkdir();
  }

  /**
   * All Positive values.
   */
  @Test
  public void testAllPos() {
    DataSetConfig config =
      DataSetConfig.getDataSetConfig(INPUT_DATASET_FOLDER_POS, DATASET_UUID_POS, OUTPUT_DATASET_FOLDER_POS);
    DwCAToAvroPaths paths = DwCAToAvroPaths.from(config.config, config.message);
    assertCases(INPUT_DATASET_FOLDER_POS, DATASET_UUID_POS, OUTPUT_DATASET_FOLDER_POS, paths);
  }

  /**
   * Wrong input dwca path.
   */
  @Test(expected = IllegalStateException.class)
  public void testInvalidInputDirectory() {
    DataSetConfig config =
      DataSetConfig.getDataSetConfig(INPUT_DATASET_FOLDER_NEG, DATASET_UUID_POS, OUTPUT_DATASET_FOLDER_POS);
    DwCAToAvroPaths.from(config.config, config.message);
  }

  /**
   * Wrong output target avro path.
   * Invalid output path is created if not available, so it does not fail.
   */
  @Test()
  public void testInvalidOutputDirectory() {
    DataSetConfig config =
      DataSetConfig.getDataSetConfig(INPUT_DATASET_FOLDER_POS, DATASET_UUID_POS, OUTPUT_DATASET_FOLDER_NEG);
    DwCAToAvroPaths paths = DwCAToAvroPaths.from(config.config, config.message);
    assertCases(INPUT_DATASET_FOLDER_POS, DATASET_UUID_POS, OUTPUT_DATASET_FOLDER_NEG, paths);
  }

  /**
   * Wrong dataset id, file is absent.
   */
  @Test(expected = IllegalStateException.class)
  public void testInvalidDatasetDirectory() {
    DataSetConfig config =
      DataSetConfig.getDataSetConfig(INPUT_DATASET_FOLDER_POS, DATASET_UUID_NEG, OUTPUT_DATASET_FOLDER_POS);
    DwCAToAvroPaths.from(config.config, config.message);
  }

  private void assertCases(String inputDatasetFolder, String datasetUUID, String outputdatasetFolder,
    DwCAToAvroPaths paths) {
    Assert.assertEquals(inputDatasetFolder + File.separator + datasetUUID, paths.getDwcaExpandedPath().toString());
    outputdatasetFolder=outputdatasetFolder.endsWith(File.separator)?outputdatasetFolder:outputdatasetFolder+File.separator;
    Assert.assertEquals(URI.create(outputdatasetFolder
                                 + datasetUUID
                                 + File.separator
                                 + "2_verbatim.avro").getPath(),
                        paths.getExtendedRepositoryExportPath().toUri().getPath());
  }

  private static class DataSetConfig {

    private final DwCAToAvroConfiguration config;
    private final DwcaValidationFinishedMessage message;

    static DataSetConfig getDataSetConfig(String inputDatasetFolder, String datasetUUID, String outputdatasetFolder) {
      return new DataSetConfig(getConfig(inputDatasetFolder, outputdatasetFolder), getMessage(datasetUUID));
    }

    /**
     * Get DwCAToAvroConfiguration based on provided parameters.
     */
    private static DwCAToAvroConfiguration getConfig(String archiveRepo, String exportAvroURL) {
      DwCAToAvroConfiguration config = new DwCAToAvroConfiguration();
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
                                               2,
                                               new DwcaValidationReport(UUID.fromString(datasetUUID), "no reason"));
    }

    private DataSetConfig(DwCAToAvroConfiguration config, DwcaValidationFinishedMessage message) {
      this.config = config;
      this.message = message;
    }
  }

}
