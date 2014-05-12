package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.StarRecord;

import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple validation of the unique identifiers of a DwC-A.
 *
 * This class is only verifying occurrence data so far and passes through checklists without an occurrence extension.
 * Records in the sense of this class are occurrence records, whether they are in the core or as an extension.
 * Therefore the number of validated records for archives with an Occurrence extension is different to the number of
 * core records or rows in the core data file.
 */
public class DwcaValidator {
  private static final Logger LOG = LoggerFactory.getLogger(DwcaValidator.class);

  // to conserve memory we won't read more than this
  private static final int MAX_RECORDS = 2000000;

  /**
   * Produce a report with the counts of good and bad unique identifiers (triplets and occurrenceId) in the archive.
   *
   *
   * @param dataset the parent Dataset of the archive
   * @param archive the archive as opened by the dwca-reader project's {@link org.gbif.dwc.text.ArchiveFactory}
   *
   * @return a report with the counts of good, bad and missing identifiers
   */
  public static DwcaValidationReport validate(Dataset dataset, Archive archive) {
    int checkedRecords = 0;
    int recordsWithInvalidTriplets = 0;
    int recordsMissingOccurrenceId = 0;

    // unique occurrenceIds
    Set<String> uniqueOccurrenceIds = Sets.newHashSet();
    // unique triplets
    Set<String> uniqueTriplets = Sets.newHashSet();

    if (dataset.getType() == DatasetType.OCCURRENCE) {
      for (StarRecord record : archive) {
        checkedRecords++;

        // triplet
        String triplet = getTriplet(record.core(), null);
        if (triplet == null) {
          recordsWithInvalidTriplets++;
        } else {
          uniqueTriplets.add(triplet);
        }

        // occurrenceId
        String occurrenceId = record.core().value(DwcTerm.occurrenceID);
        if (occurrenceId == null) {
          recordsMissingOccurrenceId++;
        } else {
          uniqueOccurrenceIds.add(occurrenceId);
        }

        if (checkedRecords == MAX_RECORDS) {
          break;
        }
      }

    } else if (archive.getExtension(DwcTerm.Occurrence) != null) {
      for (StarRecord star : archive) {
        // inner loop over occurrence extension records
        for (Record ext : star.extension(DwcTerm.Occurrence)) {
          checkedRecords++;

          // triplet can be part of both, e.g. inst and catalog number could be in the core
          String triplet = getTriplet(star.core(), ext);
          if (triplet == null) {
            recordsWithInvalidTriplets++;
          } else {
            uniqueTriplets.add(triplet);
          }

          // occurrenceId can only be in the extension
          String occurrenceId = ext.value(DwcTerm.occurrenceID);
          if (occurrenceId == null) {
            recordsMissingOccurrenceId++;
          } else {
            uniqueOccurrenceIds.add(occurrenceId);
          }

          if (checkedRecords == MAX_RECORDS) {
            break;
          }
        }
      }

    } else {
      LOG.info("Passing through DwC-A for dataset [{}] because it does not have Occurrence information to validate.",
               dataset.getKey());
      return new DwcaValidationReport(dataset.getKey(), 0, 0, 0, 0, 0, false);
    }

    return new DwcaValidationReport(dataset.getKey(), checkedRecords, uniqueTriplets.size(), recordsWithInvalidTriplets,
                                 uniqueOccurrenceIds.size(), recordsMissingOccurrenceId, checkedRecords != MAX_RECORDS);
  }

  /**
   * Creates a triplet string if pieces are found in either the core or the occurrence extension.
   * @return the triplet string or null if it cant be found
   */
  private static String getTriplet(Record core, Record ext) {
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

  private static String valueFromExtOverCore(Record core, @Nullable Record ext, Term term) {
    if (ext != null && !Strings.isNullOrEmpty(ext.value(term)) ){
      return ext.value(term);
    }
    return core.value(term);
  }
}
