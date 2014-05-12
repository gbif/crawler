package org.gbif.crawler.dwca.fragmenter;

import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.text.StarRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to serialize a dwca reader StarRecord into a simple json object.
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
   * <p/>
   * Example result:
   * <p/>
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
    // we need an alphabetically sorted map to guarantee that identical records have identical json
    Map<String, Object> data = Maps.newTreeMap();

    data.put("id", rec.core().id());

    // Put in all core terms
    for (Term term : rec.core().terms()) {
      data.put(term.simpleName(), rec.core().value(term));
    }

    if (!rec.extensions().isEmpty()) {
      Map<String, List<Map<String, String>>> extensions =
        new LinkedHashMap<String, List<Map<String, String>>>(rec.extensions().size());
      data.put("extensions", extensions);

      // iterate over extensions
      for (String rowType : rec.extensions().keySet()) {
        List<Map<String, String>> records = new ArrayList<Map<String, String>>(rec.extension(rowType).size());
        extensions.put(rowType, records);

        // iterate over extension records
        for (Record erec : rec.extension(rowType)) {
          Map<String, String> edata = new LinkedHashMap<String, String>(erec.terms().size());
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
    // we need an alphabetically sorted map to guarantee that identical records have identical json
    Map<String, Object> data = Maps.newTreeMap();

    data.put("id", core.id());

    // Put in all core terms
    for (Term term : core.terms()) {
      data.put(term.simpleName(), core.value(term));
    }

    // overlay them with extension occ terms
    for (Term term : occExtension.terms()) {
      data.put(term.simpleName(), occExtension.value(term));
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
