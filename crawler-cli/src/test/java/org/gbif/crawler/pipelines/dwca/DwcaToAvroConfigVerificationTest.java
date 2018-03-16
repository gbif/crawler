package org.gbif.crawler.pipelines.dwca;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test dwca-to-avro commands Configurations with received message parameter verification
 */
public class DwcaToAvroConfigVerificationTest {

  private static final String DATASET_UUID_POS = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DATASET_UUID_NEG = "9bed66b3-4caa-42bb-9c93-71d7ba109dae";

  private static final String DUMMY_URL = "http://some.new.url";

  private static final String INPUT_DATASET_FOLDER_POS = "dataset";
  private static final String INPUT_DATASET_FOLDER_NEG = "d";

  private static final String OUTPUT_DATASET_FOLDER_POS = "dataset/export";
  private static final String OUTPUT_DATASET_FOLDER_NEG = "dataset/e";

  @BeforeClass
  public static void init(){
    new File(OUTPUT_DATASET_FOLDER_POS).mkdir();
  }
  /**
   * All Positive values
   */
  @Test
  public void testAllPos() {
    DwCAToAvroConfiguration config = getConfig(INPUT_DATASET_FOLDER_POS, OUTPUT_DATASET_FOLDER_POS);
    DwcaValidationFinishedMessage msg = getMessage(DATASET_UUID_POS);

    DwCAToAvroCommandVerification.DwCA2AvroConfigurationParameter configParameter =
      DwCAToAvroCommandVerification.of(config).with(msg).verifyParametersAndGetResourceConfigurations();
    Assert.assertEquals(INPUT_DATASET_FOLDER_POS.concat(File.separator).concat(DATASET_UUID_POS).concat(".zip"),
                        configParameter.getAbsoluteDwCAPath());
    Assert.assertEquals(new Path(OUTPUT_DATASET_FOLDER_POS.concat(File.separator)
                                   .concat(DATASET_UUID_POS)
                                   .concat(File.separator)
                                   .concat("2_verbatim.avro")), configParameter.getAbsoluteDatasetExportPath());

  }

  /**
   * wrong input dwca path
   */
  @Test(expected = IllegalStateException.class)
  public void testInvalidInputDirectory() {
    DwCAToAvroConfiguration config = getConfig(INPUT_DATASET_FOLDER_NEG, OUTPUT_DATASET_FOLDER_POS);
    DwcaValidationFinishedMessage msg = getMessage(DATASET_UUID_POS);

    DwCAToAvroCommandVerification.DwCA2AvroConfigurationParameter configParameter =
      DwCAToAvroCommandVerification.of(config).with(msg).verifyParametersAndGetResourceConfigurations();
  }

  /**
   * wrong output target avro path
   */
  @Test(expected = IllegalStateException.class)
  public void testInvalidOutputDirectory() {
    DwCAToAvroConfiguration config = getConfig(INPUT_DATASET_FOLDER_POS, OUTPUT_DATASET_FOLDER_NEG);
    DwcaValidationFinishedMessage msg = getMessage(DATASET_UUID_POS);

    DwCAToAvroCommandVerification.DwCA2AvroConfigurationParameter configParameter =
      DwCAToAvroCommandVerification.of(config).with(msg).verifyParametersAndGetResourceConfigurations();
  }

  /**
   * wrong dataset id, file is absent
   */
  @Test(expected = IllegalStateException.class)
  public void testInvalidDatasetDirectory() {
    DwCAToAvroConfiguration config = getConfig(INPUT_DATASET_FOLDER_POS, OUTPUT_DATASET_FOLDER_POS);
    DwcaValidationFinishedMessage msg = getMessage(DATASET_UUID_NEG);

    DwCAToAvroCommandVerification.DwCA2AvroConfigurationParameter configParameter =
      DwCAToAvroCommandVerification.of(config).with(msg).verifyParametersAndGetResourceConfigurations();
  }

  /**
   * get DwCAToAvroConfiguration based on provided parameters
   */
  private DwCAToAvroConfiguration getConfig(String archiveRepo, String exportAvroURL) {
    DwCAToAvroConfiguration config = new DwCAToAvroConfiguration();
    config.archiveRepository = archiveRepo;
    config.extendedRecordRepository = exportAvroURL;
    return config;
  }

  /**
   * get DwcaValidationFinishedMessage based on provided datasetUUID
   */
  private DwcaValidationFinishedMessage getMessage(String datasetUUID) {
    return new DwcaValidationFinishedMessage(UUID.fromString(datasetUUID),
                                             DatasetType.OCCURRENCE,
                                             URI.create(DUMMY_URL),
                                             2,
                                             new DwcaValidationReport(UUID.fromString(datasetUUID), "no reason"));
  }

}
