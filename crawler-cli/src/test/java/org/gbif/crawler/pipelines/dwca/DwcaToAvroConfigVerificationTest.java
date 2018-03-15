package org.gbif.crawler.pipelines.dwca;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test dwca-to-avro commands Configurations with received message parameter verification
 */
public class DwcaToAvroConfigVerificationTest {

  private final static String DATASET_UUID_POS = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private final static String DATASET_UUID_NEG = "9bed66b3-4caa-42bb-9c93-71d7ba109dae";

  private final static String DUMMY_URL = "http://some.new.url";

  private final static String INPUT_DATASET_FOLDER_POS = "dataset";
  private final static String INPUT_DATASET_FOLDER_NEG = "d";

  private final static String OUTPUT_DATASET_FOLDER_POS = "dataset/export";
  private final static String OUTPUT_DATASET_FOLDER_NEG = "dataset/e";

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
                                   .concat("data/ingest/")
                                   .concat(DATASET_UUID_POS)
                                   .concat(File.separator)
                                   .concat("2_verbatim.avro")), configParameter.getAbsoluteDatasetExportPath());

  }

  /**
   * wrong input dwca path
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidInputDirectory() {
    DwCAToAvroConfiguration config = getConfig(INPUT_DATASET_FOLDER_NEG, OUTPUT_DATASET_FOLDER_POS);
    DwcaValidationFinishedMessage msg = getMessage(DATASET_UUID_POS);

    DwCAToAvroCommandVerification.DwCA2AvroConfigurationParameter configParameter =
      DwCAToAvroCommandVerification.of(config).with(msg).verifyParametersAndGetResourceConfigurations();
  }

  /**
   * wrong output target avro path
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOutputDirectory() {
    DwCAToAvroConfiguration config = getConfig(INPUT_DATASET_FOLDER_POS, OUTPUT_DATASET_FOLDER_NEG);
    DwcaValidationFinishedMessage msg = getMessage(DATASET_UUID_POS);

    DwCAToAvroCommandVerification.DwCA2AvroConfigurationParameter configParameter =
      DwCAToAvroCommandVerification.of(config).with(msg).verifyParametersAndGetResourceConfigurations();
  }

  /**
   * wrong dataset id, file is absent
   */
  @Test(expected = IllegalArgumentException.class)
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
    config.exportAvroBaseURL = exportAvroURL;
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
