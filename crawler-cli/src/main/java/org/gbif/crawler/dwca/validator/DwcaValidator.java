package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.ChecklistValidationReport;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.StarRecord;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple validation of the unique identifiers of a DwC-A.
 * <p/>
 * This class is verifying occurrence and taxon data and marks other datasets as invalids.
 * Occurrence records are checked whether they are in the core or as an extension.
 * Therefore the number of validated records for archives with an Occurrence extension is different to the number of
 * core records or rows in the core data file.
 */
public class DwcaValidator {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaValidator.class);

  // to conserve memory we won't read more than this
  private static final int MAX_RECORDS = 2000000;
  private static final int MAX_DUPLICATES = 100;

  private int checkedRecords = 0;
  private int recordsWithInvalidTriplets = 0;
  private int recordsMissingOccurrenceId = 0;
  // unique occurrenceIds
  private Set<String> uniqueOccurrenceIds = Sets.newHashSet();
  // unique triplets
  private Set<String> uniqueTriplets = Sets.newHashSet();

  private DwcaValidator() {
  }

  /**
   * Produce a report with the counts of good and bad unique identifiers (triplets and occurrenceId) in the archive.
   *
   * @param dataset the parent Dataset of the archive
   * @param archive the archive as opened by the dwca-reader project's {@link org.gbif.dwc.text.ArchiveFactory}
   *
   * @return a report with the counts of good, bad and missing identifiers
   */
  public static DwcaValidationReport validate(Dataset dataset, Archive archive) {
    DwcaValidator validator = new DwcaValidator();
    return validator.check(dataset, archive);
  }

  /**
   * Internal non static working method that does the validation.
   * If an occurrence core is found this is what gets validated.
   * Otherwise extension records from either dwc:Occurrence or gbif:TypesAndSpecimen are validated
   * with Occurrence being the preferred extension.
   */
  private DwcaValidationReport check(Dataset dataset, Archive archive) {
    if (dataset.getType() == DatasetType.OCCURRENCE) {
      return new DwcaValidationReport(dataset.getKey(), validateOccurrenceCore(archive));

    } else if (dataset.getType() == DatasetType.CHECKLIST) {
      ChecklistValidationReport taxonReport = validateTaxonCore(archive);
      if (archive.getExtension(DwcTerm.Occurrence) == null) {
        return new DwcaValidationReport(dataset.getKey(), taxonReport);
      } else {
        return new DwcaValidationReport(dataset.getKey(), validateOccurrenceExtension(archive, DwcTerm.Occurrence), taxonReport);
      }

    } else {
      LOG.info("Passing through DwC-A for dataset [{}] because it does not have any information to validate.", dataset.getKey());
      return new DwcaValidationReport(dataset.getKey(), "No Occurrence or Taxon information present");
    }
  }

  private ChecklistValidationReport validateTaxonCore(Archive archive) {
    int records = 0;
    List<String> duplicateIds = Lists.newArrayList();
    List<Integer> linesMissingIds = Lists.newArrayList();
    Set<String> ids = Sets.newHashSet();
    final boolean useCoreID = !archive.getCore().hasTerm(DwcTerm.taxonID);
    for (StarRecord rec: archive) {
      records++;
      String id = useCoreID ? rec.core().id() : rec.core().value(DwcTerm.taxonID);
      if (linesMissingIds.size() < MAX_DUPLICATES && Strings.isNullOrEmpty(id)) {
        linesMissingIds.add(records);
      }
      if (duplicateIds.size() < MAX_DUPLICATES && ids.contains(id)) {
        duplicateIds.add(id);
      }

      if (!Strings.isNullOrEmpty(id) && ids.size() < MAX_RECORDS) {
        ids.add(id);
      }
    }
    return new ChecklistValidationReport(records, records!=MAX_RECORDS, duplicateIds, linesMissingIds);
  }

  private OccurrenceValidationReport validateOccurrenceCore(Archive archive) {
    for (StarRecord record : archive) {
      if (checkOccurrenceRecord(record.core(), getTriplet(record.core(), null))) {
        break;
      }
    }
    return new OccurrenceValidationReport(checkedRecords, uniqueTriplets.size(), recordsWithInvalidTriplets,
      uniqueOccurrenceIds.size(), recordsMissingOccurrenceId, checkedRecords != MAX_RECORDS);
  }

  private OccurrenceValidationReport validateOccurrenceExtension(Archive archive, final Term rowType) {
    // outer loop over core records, e.g. taxa or samples
    for (StarRecord star : archive) {
      // inner loop over extension records
      List<Record> records = star.extension(rowType);
      if (records != null) {
        for (Record ext : records) {
          if (checkOccurrenceRecord(ext, getTriplet(star.core(), ext))) {
            break;
          }
        }
      }
    }
    return new OccurrenceValidationReport(checkedRecords, uniqueTriplets.size(), recordsWithInvalidTriplets,
      uniqueOccurrenceIds.size(), recordsMissingOccurrenceId, checkedRecords != MAX_RECORDS);
  }

  private boolean checkOccurrenceRecord(Record rec, String triplet) {
    checkedRecords++;

    // triplet can be part of both, e.g. inst and catalog number could be in the core
    if (triplet == null) {
      recordsWithInvalidTriplets++;
    } else {
      uniqueTriplets.add(triplet);
    }

    // occurrenceId can only be in the extension
    String occurrenceId = rec.value(DwcTerm.occurrenceID);
    if (occurrenceId == null) {
      recordsMissingOccurrenceId++;
    } else {
      uniqueOccurrenceIds.add(occurrenceId);
    }

    if (checkedRecords == MAX_RECORDS) {
      return true;
    }
    return false;
  }

  /**
   * Creates a triplet string if pieces are found in either the core or the occurrence extension.
   *
   * @return the triplet string or null if it cant be found
   */
  private String getTriplet(Record core, Record ext) {
    String institutionCode = valueFromExtOverCore(core, ext, DwcTerm.institutionCode);
    String collectionCode = valueFromExtOverCore(core, ext, DwcTerm.collectionCode);
    String catalogNumber = valueFromExtOverCore(core, ext, DwcTerm.catalogNumber);

    if (!Strings.isNullOrEmpty(institutionCode) &&
        !Strings.isNullOrEmpty(collectionCode) &&
        !Strings.isNullOrEmpty(catalogNumber)) {
      return institutionCode + "§" + collectionCode + "§" + catalogNumber;
    }
    return null;
  }

  private String valueFromExtOverCore(Record core, @Nullable Record ext, Term term) {
    if (ext != null && !Strings.isNullOrEmpty(ext.value(term))) {
      return ext.value(term);
    }
    return core.value(term);
  }
}
