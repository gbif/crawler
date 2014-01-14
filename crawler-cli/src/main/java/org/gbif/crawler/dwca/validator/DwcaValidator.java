package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.StarRecord;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * Simple validation of the unique identifiers of a DwC-A.
 */
public class DwcaValidator {

  // to conserve memory we won't read more than this
  private static final int MAX_RECORDS = 2000000;

  /**
   * Produce a report with the counts of good and bad unique identifiers (triplets and occurrenceId) in the archive.
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

    for (StarRecord record : archive) {
      checkedRecords++;

      // triplet
      String institutionCode = record.core().value(DwcTerm.institutionCode);
      String collectionCode = record.core().value(DwcTerm.collectionCode);
      String catalogNumber = record.core().value(DwcTerm.catalogNumber);
      if (institutionCode == null || institutionCode.isEmpty() || collectionCode == null || collectionCode.isEmpty()
          || catalogNumber == null || catalogNumber.isEmpty()) {
        recordsWithInvalidTriplets++;
      } else {
        String triplet = institutionCode + collectionCode + catalogNumber;
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

    return new DwcaValidationReport(dataset.getKey(), checkedRecords, uniqueTriplets.size(), recordsWithInvalidTriplets,
      uniqueOccurrenceIds.size(), recordsMissingOccurrenceId, checkedRecords != MAX_RECORDS);
  }
}
