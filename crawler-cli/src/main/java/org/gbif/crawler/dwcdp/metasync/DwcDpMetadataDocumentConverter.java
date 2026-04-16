package org.gbif.crawler.dwcdp.metasync;

import org.gbif.api.vocabulary.MetadataType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extracts metadata documents from a DwcDP archive.
 *
 * <p>Always extracts {@code datapackage.json} (mandatory in DwcDP). Additionally extracts {@code
 * eml.xml} when present; EML is the richer, registry-preferred format and should be used as the
 * primary metadata document when available.
 */
public class DwcDpMetadataDocumentConverter {

  private static final List<String> DATAPACKAGE_NAMES = List.of("datapackage.json");
  private static final List<String> EML_NAMES = List.of("eml.xml");

  private final ObjectMapper jsonMapper = new ObjectMapper();

  public DwcDpMetadataExtractionResult extractDocuments(File archive) throws IOException {
    byte[] datapackageBytes = null;
    byte[] emlBytes = null;

    // Single pass: read all relevant entry bytes eagerly so no sequential getInputStream issues.
    try (ZipFile zipFile = new ZipFile(archive)) {
      var entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (entry.isDirectory()) {
          continue;
        }
        String name = entry.getName().toLowerCase(Locale.ROOT);
        if (datapackageBytes == null && matchesName(name, DATAPACKAGE_NAMES)) {
          try (InputStream is = zipFile.getInputStream(entry)) {
            datapackageBytes = is.readAllBytes();
          }
        } else if (emlBytes == null && matchesName(name, EML_NAMES)) {
          try (InputStream is = zipFile.getInputStream(entry)) {
            emlBytes = is.readAllBytes();
          }
        }
      }
    }

    if (datapackageBytes == null) {
      throw new IOException("No datapackage.json found in " + archive);
    }

    String contentJson = jsonMapper.writeValueAsString(jsonMapper.readTree(datapackageBytes));
    DwcDpMetadataDocument datapackageDocument =
        new DwcDpMetadataDocument(datapackageBytes, contentJson, MetadataType.DWC_DP);

    DwcDpMetadataDocument emlDocument =
        emlBytes != null ? new DwcDpMetadataDocument(emlBytes, null, MetadataType.EML) : null;

    return new DwcDpMetadataExtractionResult(datapackageDocument, emlDocument);
  }

  private boolean matchesName(String normalizedEntryName, List<String> acceptedNames) {
    for (String acceptedName : acceptedNames) {
      String lowered = acceptedName.toLowerCase(Locale.ROOT);
      if (normalizedEntryName.equals(lowered) || normalizedEntryName.endsWith("/" + lowered)) {
        return true;
      }
    }
    return false;
  }
}
