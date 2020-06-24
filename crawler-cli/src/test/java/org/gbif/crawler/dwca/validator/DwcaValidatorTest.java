/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DwcaValidatorTest {

  private Dataset dataset;

  @Before
  public void setUp() {
    dataset = new Dataset();
    dataset.setKey(UUID.randomUUID());
    dataset.setType(DatasetType.OCCURRENCE);
  }

  @Test
  @Ignore("manual test to validate archives")
  public void manualUrlTest() throws IOException {
    URI dwca = URI.create("http://pensoft.net/dwc/bdj/checklist_980.zip");

    File tmp = File.createTempFile("gbif", "dwca");
    tmp.deleteOnExit();
    File dwcaDir = org.gbif.utils.file.FileUtils.createTempDir();
    dwcaDir.deleteOnExit();

    FileUtils.copyURLToFile(dwca.toURL(), tmp);

    Archive archive = DwcFiles.fromCompressed(tmp.toPath(), dwcaDir.toPath());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    System.out.println(report);
  }

  @Test
  public void testGoodTripletsGoodIds() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(100, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(
        report.getOccurrenceReport().getUniqueTriplets(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(
        report.getOccurrenceReport().getUniqueOccurrenceIds(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testChecklistGoodTripletsGoodIds() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);

    Archive archive =
        DwcaTestUtil.openArchive("/dwca/dwca_checklist-one-hundred-good-triplets-good-ids.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(100, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(
        report.getOccurrenceReport().getUniqueTriplets(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(
        report.getOccurrenceReport().getUniqueOccurrenceIds(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testEmlOnly() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/eml.xml");
    dataset.setType(DatasetType.METADATA);
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertTrue(report.isValid());
  }

  @Test
  public void testGoodTripletsNoOccurrenceId() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-thousand-good-triplets-no-id.zip");

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(1000, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(1000, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(1000, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(
        report.getOccurrenceReport().getUniqueTriplets(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(
        report.getOccurrenceReport().getUniqueOccurrenceIds(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testDupeTriplet() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-dupe-triplet.zip");

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(10, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testInvalidTripletInValidArchive() throws IOException {
    Archive archive =
        DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-20-percent-invalid-triplet.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(80, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(20, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testGoodTripletsDupedIds() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-dupe-ids.zip");

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(90, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testGoodTripletsDupedAndMissingIds() throws IOException {
    Archive archive =
        DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-dupe-and-missing-ids.zip");

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(80, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(10, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testInvalidAndDupeTriplet() throws IOException {
    Archive archive =
        DwcaTestUtil.openArchive(
            "/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(5, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(50, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertEquals(
        "Archive invalid because [50% invalid triplets is > than threshold of 25%; 45 duplicate triplets detected; 100 records without an occurrence id (should be 0)]",
        report.getOccurrenceReport().getInvalidationReason());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testDupeAndBadTripletNoOccurrenceId() throws IOException {
    Archive archive =
        DwcaTestUtil.openArchive(
            "/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(5, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(50, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertEquals(
        "Archive invalid because [50% invalid triplets is > than threshold of 25%; 45 duplicate triplets detected; 100 records without an occurrence id (should be 0)]",
        report.getOccurrenceReport().getInvalidationReason());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testEmptyArchive() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-empty.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(0, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(0, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertNull(report.getOccurrenceReport().getInvalidationReason());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testGoodChecklistTaxonID() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_good_taxonid.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertTrue("Validation failed: " + report.getInvalidationReason(), report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertTrue(report.getGenericReport().getDuplicateIds().isEmpty());
    assertTrue(report.getGenericReport().getRowNumbersMissingId().isEmpty());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testGoodGenericCore() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_good_coreid.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertTrue("Validation failed: " + report.getInvalidationReason(), report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertEquals(0, report.getGenericReport().getDuplicateIds().size());
    assertEquals(0, report.getGenericReport().getRowNumbersMissingId().size());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testBadGenericMissing() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_missing_taxonid.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertFalse("Validation succeeded", report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertEquals(0, report.getGenericReport().getDuplicateIds().size());
    assertEquals(2, report.getGenericReport().getRowNumbersMissingId().size());

    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testBadGenericDupl() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_dupl_coreid.zip");
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertFalse("Validation succeeded", report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertEquals(1, report.getGenericReport().getDuplicateIds().size());
    assertEquals(0, report.getGenericReport().getRowNumbersMissingId().size());

    DwcaTestUtil.cleanupArchive(archive);
  }
}
