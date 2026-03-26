package org.gbif.crawler.coldp.metasync;

import org.gbif.crawler.coldp.metadata.ColDpMetadata;
import org.gbif.crawler.util.TemplateUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import freemarker.template.TemplateException;

/**
 * Generates a minimal EML document derived from ColDP metadata for registry metadata endpoints.
 */
public class ColDpEmlWriter {

  private static final String DEFAULT_LANGUAGE = "eng";
  private static final String TEMPLATE = "template/coldp/eml.ftl";

  private ColDpEmlWriter() {}

  public static InputStream toInputStream(UUID datasetKey, ColDpMetadata metadata) throws IOException {
    return new ByteArrayInputStream(toXml(datasetKey, metadata).getBytes(StandardCharsets.UTF_8));
  }

  static String toXml(UUID datasetKey, ColDpMetadata metadata) throws IOException {
    try {
      return TemplateUtils.getAndProcess(TEMPLATE, buildTemplateModel(datasetKey, metadata));
    } catch (TemplateException e) {
      throw new IOException("Failed to render COLDP EML for dataset " + datasetKey, e);
    }
  }

  private static Map<String, Object> buildTemplateModel(UUID datasetKey, ColDpMetadata metadata) {
    Map<String, Object> model = new LinkedHashMap<>();
    model.put("packageId", "https://api.gbif.org/dataset/" + datasetKey + "/metadata/coldp-generated");
    model.put("language", normalizeLanguage(metadata.getLanguage()));
    model.put("title", firstNonBlank(metadata.getTitle(), datasetKey.toString()));
    model.put("pubDate", normalizeDate(metadata.getIssued()));
    model.put("description", buildDescription(metadata));
    model.put("rights", buildRightsParagraph(metadata));
    model.put("dateStamp", OffsetDateTime.now().toString());
    model.put("creators", metadata.getCreators());
    model.put("contributors", metadata.getContributors());
    model.put("metadataProvider", chooseMetadataProvider(metadata));
    model.put("contact", chooseContact(metadata));
    model.put("alternateIdentifiers", buildAlternateIdentifiers(metadata));
    return model;
  }

  private static List<String> buildAlternateIdentifiers(ColDpMetadata metadata) {
    List<String> identifiers = new ArrayList<>();
    if (trimToNull(metadata.getDoi()) != null) {
      identifiers.add("doi:" + metadata.getDoi().trim());
    }
    if (metadata.getIdentifiers() != null) {
      metadata
          .getIdentifiers()
          .forEach((key, value) -> {
            if (trimToNull(key) != null && trimToNull(value) != null) {
              identifiers.add(key.trim() + ":" + value.trim());
            }
          });
    }
    return identifiers;
  }

  private static ColDpMetadata.Agent chooseMetadataProvider(ColDpMetadata metadata) {
    if (metadata.getPublisher() != null) {
      return metadata.getPublisher();
    }
    if (!metadata.getEditors().isEmpty()) {
      return metadata.getEditors().get(0);
    }
    if (!metadata.getCreators().isEmpty()) {
      return metadata.getCreators().get(0);
    }
    return chooseContact(metadata);
  }

  private static ColDpMetadata.Agent chooseContact(ColDpMetadata metadata) {
    if (!metadata.getContacts().isEmpty()) {
      return metadata.getContacts().get(0);
    }
    if (metadata.getPublisher() != null) {
      return metadata.getPublisher();
    }
    if (!metadata.getCreators().isEmpty()) {
      return metadata.getCreators().get(0);
    }
    return null;
  }

  private static String buildDescription(ColDpMetadata metadata) {
    List<String> parts = new ArrayList<>();
    if (trimToNull(metadata.getDescription()) != null) {
      parts.add(metadata.getDescription().trim());
    }
    if (trimToNull(metadata.getNotes()) != null) {
      parts.add(metadata.getNotes().trim());
    }
    if (parts.isEmpty()) {
      return null;
    }
    return String.join("\n\n", parts);
  }

  private static String buildRightsParagraph(ColDpMetadata metadata) {
    List<String> parts = new ArrayList<>();
    if (trimToNull(metadata.getLicense()) != null) {
      parts.add("License: " + metadata.getLicense().trim());
    }
    if (trimToNull(metadata.getUrl()) != null) {
      parts.add("Source URL: " + metadata.getUrl().trim());
    }
    return parts.isEmpty() ? null : String.join(". ", parts);
  }

  private static String normalizeDate(String issued) {
    String value = trimToNull(issued);
    if (value == null) {
      return null;
    }
    try {
      return OffsetDateTime.parse(value).toLocalDate().toString();
    } catch (DateTimeParseException e) {
      return value;
    }
  }

  private static String normalizeLanguage(String language) {
    String value = trimToNull(language);
    if (value == null) {
      return DEFAULT_LANGUAGE;
    }
    if (value.length() == 3) {
      return value.toLowerCase();
    }
    if ("en".equalsIgnoreCase(value)) {
      return DEFAULT_LANGUAGE;
    }
    return value.toLowerCase();
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      String trimmed = trimToNull(value);
      if (trimmed != null) {
        return trimmed;
      }
    }
    return null;
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
