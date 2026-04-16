package org.gbif.crawler.coldp.metasync;

import org.gbif.api.vocabulary.MetadataType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Extracts metadata documents from a COLDP archive.
 *
 * <p>Always extracts the ColDP format document (metadata.json, metadata.yaml, or metadata.yml).
 * Additionally extracts {@code eml.xml} as EML when present; EML is the richer,
 * registry-preferred format and should be used as the primary metadata document when available.
 */
public class ColDpMetadataDocumentConverter {

  private static final List<String> YAML_NAMES = List.of("metadata.yaml", "metadata.yml");
  private static final List<String> JSON_NAMES = List.of("metadata.json");
  private static final List<String> EML_NAMES = List.of("eml.xml");

  private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
  private final ObjectMapper jsonMapper = new ObjectMapper();

  public ColDpMetadataExtractionResult extractDocuments(File archive) throws IOException {
    byte[] formatBytes = null;
    boolean isYaml = false;
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
        if (formatBytes == null && matchesName(name, JSON_NAMES)) {
          try (InputStream is = zipFile.getInputStream(entry)) {
            formatBytes = is.readAllBytes();
          }
          isYaml = false;
        } else if (formatBytes == null && matchesName(name, YAML_NAMES)) {
          try (InputStream is = zipFile.getInputStream(entry)) {
            formatBytes = is.readAllBytes();
          }
          isYaml = true;
        } else if (emlBytes == null && matchesName(name, EML_NAMES)) {
          try (InputStream is = zipFile.getInputStream(entry)) {
            emlBytes = is.readAllBytes();
          }
        }
      }
    }

    if (formatBytes == null) {
      throw new IOException(
          "No metadata.yaml, metadata.yml or metadata.json found in " + archive);
    }

    JsonNode tree = isYaml ? yamlMapper.readTree(formatBytes) : jsonMapper.readTree(formatBytes);
    String contentJson = jsonMapper.writeValueAsString(tree);
    ColDpMetadataDocument formatDocument =
        new ColDpMetadataDocument(formatBytes, contentJson, MetadataType.COLDP);

    ColDpMetadataDocument emlDocument =
        emlBytes != null ? new ColDpMetadataDocument(emlBytes, null, MetadataType.EML) : null;

    return new ColDpMetadataExtractionResult(formatDocument, emlDocument);
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
