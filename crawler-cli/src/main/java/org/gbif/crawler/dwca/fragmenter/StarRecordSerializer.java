package org.gbif.crawler.dwca.fragmenter;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to serialize a DwCA reader StarRecord into a simple json object.
 */
final class StarRecordSerializer {

  private static final Logger LOG = LoggerFactory.getLogger(StarRecordSerializer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // to ensure that identical records are serialized identically
    MAPPER.configure(SerializationConfig.Feature.SORT_PROPERTIES_ALPHABETICALLY, true);
  }

  /**
   * Creates a json object for the complete, verbatim star record.
   * The main object represents the core record, with the simple, unqualified term name being the key.
   * Extension records are similar objects themselves and are lists keyed on the extension rowType
   * in the main extensions.
   * Example result:
   * {"id":"100",
   * "taxonomicStatus":"valid",
   * "taxonRank":"Species",
   * "scientificNameAuthorship":null,
   * "parentNameUsageID":"86",
   * "acceptedNameUsageID":null,
   * "scientificName":"Spirillum beijerinckii",
   * "extensions": {
   * "VernacularName" : [{"vernacularName":"", "language":"en", ...}, {...}],
   * "Distribution" : [{...}, {...}}],
   * }
   *
   * @param rec the star record to serialize
   *
   * @return the json string
   */
  public static byte[] toJson(StarRecord rec) {
    // we need alphabetically sorted maps to guarantee that identical records have identical JSON
    Map<String, Object> data = new TreeMap<>();

    data.put("id", rec.core().id());

    // Put in all core terms
    for (Term term : rec.core().terms()) {
      data.put(term.simpleName(), rec.core().value(term));
    }

    if (!rec.extensions().isEmpty()) {
      SortedMap<Term, List<Map<String, String>>> extensions = new TreeMap<>(Comparator.comparing(Term::qualifiedName));
      data.put("extensions", extensions);

      // iterate over extensions
      for (Term rowType : rec.extensions().keySet()) {
        List<Map<String, String>> records = new ArrayList<>(rec.extension(rowType).size());
        extensions.put(rowType, records);

        // iterate over extension records
        for (Record erec : rec.extension(rowType)) {
          Map<String, String> edata = new TreeMap<>();
          records.add(edata);
          for (Term term : erec.terms()) {
            edata.put(term.simpleName(), erec.value(term));
          }
        }
      }
    }
    // serialize to json
    try {
      return MAPPER.writeValueAsBytes(data);
    } catch (IOException e) {
      LOG.error("Cannot serialize star record data", e);
    }
    return null;
  }

  public static byte[] toJson(Record core, Record occExtension) {
    // we need alphabetically sorted maps to guarantee that identical records have identical JSON
    Map<String, Object> data = new TreeMap<>();

    data.put("id", core.id());

    // Put in all core terms
    for (Term term : core.terms()) {
      data.put(term.simpleName(), core.value(term));
    }

    // overlay them with extension occ terms
    for (Term term : occExtension.terms()) {
      // do not overwrite values with a NULL.  It can be the case that e.g. Taxon core has values, while the extension
      // declares the same terms, but provides no value.
      if (!StringUtils.isBlank(occExtension.value(term))) {
        data.put(term.simpleName(), occExtension.value(term));
      }
    }

    // serialize to json
    try {
      return MAPPER.writeValueAsBytes(data);
    } catch (IOException e) {
      LOG.error("Cannot serialize star record data", e);
    }
    return null;
  }

  private StarRecordSerializer() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

}
