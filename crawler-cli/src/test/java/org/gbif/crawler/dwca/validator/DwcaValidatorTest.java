package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;

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

    Archive archive = ArchiveFactory.openArchive(tmp, dwcaDir);
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    System.out.println(report);
  }

  @Test
  public void testGoodTripletsGoodIds() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(100, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(report.getOccurrenceReport().getUniqueTriplets(),
      report.getOccurrenceReport().getCheckedRecords() - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(report.getOccurrenceReport().getUniqueOccurrenceIds(),
      report.getOccurrenceReport().getCheckedRecords() - report.getOccurrenceReport().getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testChecklistGoodTripletsGoodIds() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);

    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca_checklist-one-hundred-good-triplets-good-ids.zip");
    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(100, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(report.getOccurrenceReport().getUniqueTriplets(),
      report.getOccurrenceReport().getCheckedRecords() - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(report.getOccurrenceReport().getUniqueOccurrenceIds(),
      report.getOccurrenceReport().getCheckedRecords() - report.getOccurrenceReport().getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }


  @Test
  public void testGoodTripletsNoOccurrenceId() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-thousand-good-triplets-no-id.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(1000, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(1000, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(1000, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(report.getOccurrenceReport().getUniqueTriplets(),
      report.getOccurrenceReport().getCheckedRecords() - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(report.getOccurrenceReport().getUniqueOccurrenceIds(),
      report.getOccurrenceReport().getCheckedRecords() - report.getOccurrenceReport().getRecordsMissingOccurrenceId());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testDupeTriplet() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-dupe-triplet.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(10, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testInvalidTripletInValidArchive() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-20-percent-invalid-triplet.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(80, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(20, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testGoodTripletsDupedIds() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-good-triplets-dupe-ids.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(90, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testGoodTripletsDupedAndMissingIds() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-good-triplets-dupe-and-missing-ids.zip");
    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());

    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(80, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(10, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testInvalidAndDupeTriplet() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");
    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());

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

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testDupeAndBadTripletNoOccurrenceId() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
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

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  @Test
  public void testEmptyArchive() throws IOException {
    File zip = DwcaTestUtil.copyTestArchive("/dwca/dwca-empty.zip");

    Archive archive = ArchiveFactory.openArchive(zip, zip.getParentFile());
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertEquals(0, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(0, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertNull(report.getOccurrenceReport().getInvalidationReason());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }


  @Test
  public void testGoodChecklistTaxonID() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    File zip = DwcaTestUtil.copyTestArchive("/dwca/checklist_good_taxonid.zip");

    Archive archive = ArchiveFactory.openArchive(zip, addExpandFolder(zip));
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertTrue("Validation failed: " + report.getInvalidationReason(), report.isValid());
    assertEquals(15, report.getChecklistReport().getCheckedRecords());
    assertTrue(report.getChecklistReport().getDuplicateIds().isEmpty());
    assertTrue(report.getChecklistReport().getMissingIds().isEmpty());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }


  @Test
  public void testGoodChecklistCore() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    File zip = DwcaTestUtil.copyTestArchive("/dwca/checklist_good_coreid.zip");

    Archive archive = ArchiveFactory.openArchive(zip, addExpandFolder(zip));
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertTrue("Validation failed: " + report.getInvalidationReason(), report.isValid());
    assertEquals(15, report.getChecklistReport().getCheckedRecords());
    assertEquals(0, report.getChecklistReport().getDuplicateIds().size());
    assertEquals(0, report.getChecklistReport().getMissingIds().size());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }


  @Test
  public void testBadChecklistMissing() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    File zip = DwcaTestUtil.copyTestArchive("/dwca/checklist_missing_taxonid.zip");

    Archive archive = ArchiveFactory.openArchive(zip, addExpandFolder(zip));
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertFalse("Validation succeeded", report.isValid());
    assertEquals(15, report.getChecklistReport().getCheckedRecords());
    assertEquals(0, report.getChecklistReport().getDuplicateIds().size());
    assertEquals(2, report.getChecklistReport().getMissingIds().size());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }


  @Test
  public void testBadChecklistDupl() throws IOException {
    dataset.setType(DatasetType.CHECKLIST);
    File zip = DwcaTestUtil.copyTestArchive("/dwca/checklist_dupl_coreid.zip");

    Archive archive = ArchiveFactory.openArchive(zip, addExpandFolder(zip));
    DwcaValidationReport report = DwcaValidator.validate(dataset, archive);
    assertFalse("Validation succeeded", report.isValid());
    assertEquals(15, report.getChecklistReport().getCheckedRecords());
    assertEquals(1, report.getChecklistReport().getDuplicateIds().size());
    assertEquals(0, report.getChecklistReport().getMissingIds().size());

    DwcaTestUtil.cleanupArchive(zip.getParent());
  }

  private File addExpandFolder(File dwca) {
    File tmp = new File(dwca.getParentFile(), "expanded");
    tmp.mkdir();
    return tmp;
  }
}
