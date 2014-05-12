package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Before;
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
  public void testGoodTripletsGoodIds() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(100, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(100, report.getUniqueOccurrenceIds());
    assertEquals(0, report.getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.isAllRecordsChecked());
    assertEquals(report.getUniqueTriplets(), report.getCheckedRecords() - report.getRecordsWithInvalidTriplets());
    assertEquals(report.getUniqueOccurrenceIds(), report.getCheckedRecords() - report.getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testChecklistGoodTripletsGoodIds() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);

    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca_checklist-one-hundred-good-triplets-good-ids.zip");
    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(100, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(100, report.getUniqueOccurrenceIds());
    assertEquals(0, report.getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.isAllRecordsChecked());
    assertEquals(report.getUniqueTriplets(), report.getCheckedRecords() - report.getRecordsWithInvalidTriplets());
    assertEquals(report.getUniqueOccurrenceIds(), report.getCheckedRecords() - report.getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }


  @Test
  public void testGoodTripletsNoOccurrenceId() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-thousand-good-triplets-no-id.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(1000, report.getCheckedRecords());
    assertEquals(1000, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(0, report.getUniqueOccurrenceIds());
    assertEquals(1000, report.getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.isAllRecordsChecked());
    assertEquals(report.getUniqueTriplets(), report.getCheckedRecords() - report.getRecordsWithInvalidTriplets());
    assertEquals(report.getUniqueOccurrenceIds(), report.getCheckedRecords() - report.getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testDupeTriplet() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-dupe-triplet.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(10, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(0, report.getUniqueOccurrenceIds());
    assertEquals(100, report.getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertTrue(report.isAllRecordsChecked());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testInvalidTripletInValidArchive() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-20-percent-invalid-triplet.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(80, report.getUniqueTriplets());
    assertEquals(20, report.getRecordsWithInvalidTriplets());
    assertEquals(0, report.getUniqueOccurrenceIds());
    assertEquals(100, report.getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testGoodTripletsDupedIds() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-good-triplets-dupe-ids.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(100, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(90, report.getUniqueOccurrenceIds());
    assertEquals(0, report.getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testGoodTripletsDupedAndMissingIds() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-good-triplets-dupe-and-missing-ids.zip");
    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(100, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(80, report.getUniqueOccurrenceIds());
    assertEquals(10, report.getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testInvalidAndDupeTriplet() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");
    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(5, report.getUniqueTriplets());
    assertEquals(50, report.getRecordsWithInvalidTriplets());
    assertEquals(0, report.getUniqueOccurrenceIds());
    assertEquals(100, report.getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertEquals(
      "Archive invalid because [50% invalid triplets is > than threshold of 25%; 45 duplicate triplets detected; 100 records without an occurrence id (should be 0)]",
      report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testDupeAndBadTripletNoOccurrenceId() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getCheckedRecords());
    assertEquals(5, report.getUniqueTriplets());
    assertEquals(50, report.getRecordsWithInvalidTriplets());
    assertEquals(0, report.getUniqueOccurrenceIds());
    assertEquals(100, report.getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertEquals(
      "Archive invalid because [50% invalid triplets is > than threshold of 25%; 45 duplicate triplets detected; 100 records without an occurrence id (should be 0)]",
      report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testEmptyArchive() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-empty.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(0, report.getCheckedRecords());
    assertEquals(0, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(0, report.getUniqueOccurrenceIds());
    assertEquals(0, report.getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertEquals("Archive invalid because [No readable records]", report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }
}
