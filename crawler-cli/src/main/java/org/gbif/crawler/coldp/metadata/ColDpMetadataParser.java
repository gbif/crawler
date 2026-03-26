package org.gbif.crawler.coldp.metadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Parser for the current ColDP metadata shape with limited backward-compatible handling for agent
 * fields that may appear as single objects, arrays, or simple strings.
 */
public class ColDpMetadataParser {

  private static final Logger LOG = LoggerFactory.getLogger(ColDpMetadataParser.class);
  private static final List<String> YAML_NAMES = List.of("metadata.yaml", "metadata.yml");
  private static final String JSON_NAME = "metadata.json";

  private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
  private final ObjectMapper jsonMapper = new ObjectMapper();

  public ColDpMetadata parseArchive(File archive) throws IOException {
    try (ZipFile zipFile = new ZipFile(archive)) {
      ZipEntry metadataEntry = findEntry(zipFile, YAML_NAMES);
      if (metadataEntry != null) {
        try (InputStream inputStream = zipFile.getInputStream(metadataEntry)) {
          return parseYaml(inputStream);
        }
      }

      metadataEntry = findEntry(zipFile, List.of(JSON_NAME));
      if (metadataEntry != null) {
        LOG.info("Falling back to metadata.json for archive {}", archive.getName());
        try (InputStream inputStream = zipFile.getInputStream(metadataEntry)) {
          return parseJson(inputStream);
        }
      }
    }

    throw new IOException("No metadata.yaml or metadata.json found in " + archive.getAbsolutePath());
  }

  public ColDpMetadata parseYaml(InputStream inputStream) throws IOException {
    return parseTree(yamlMapper.readTree(inputStream));
  }

  public ColDpMetadata parseJson(InputStream inputStream) throws IOException {
    return parseTree(jsonMapper.readTree(inputStream));
  }

  ColDpMetadata parseTree(JsonNode root) {
    ColDpMetadata metadata = new ColDpMetadata();
    metadata.setTitle(asText(root.get("title")));
    metadata.setDescription(asText(root.get("description")));
    metadata.setDoi(asText(root.get("doi")));
    metadata.setIssued(asText(root.get("issued")));
    metadata.setVersion(asText(root.get("version")));
    metadata.setLicense(asText(root.get("license")));
    metadata.setUrl(asText(root.get("url")));
    metadata.setLogo(asText(root.get("logo")));
    metadata.setLanguage(asText(firstPresent(root, "language", "defaultLanguage")));
    metadata.setNotes(asText(root.get("notes")));
    metadata.setIdentifiers(readIdentifiers(root.get("identifier")));

    addAgents(root.get("contact"), metadata.getContacts()::add);
    addAgents(root.get("creator"), metadata.getCreators()::add);
    addAgents(root.get("editor"), metadata.getEditors()::add);
    addAgents(root.get("contributor"), metadata.getContributors()::add);
    metadata.setPublisher(readAgent(root.get("publisher")));
    return metadata;
  }

  private Map<String, String> readIdentifiers(JsonNode identifiersNode) {
    if (identifiersNode == null || identifiersNode.isNull() || !identifiersNode.isObject()) {
      return Map.of();
    }

    Map<String, String> identifiers = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = identifiersNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String value = asText(field.getValue());
      if (value != null) {
        identifiers.put(field.getKey(), value);
      }
    }
    return identifiers;
  }

  private void addAgents(JsonNode node, Consumer<ColDpMetadata.Agent> sink) {
    if (node == null || node.isNull()) {
      return;
    }
    if (node.isArray()) {
      for (JsonNode child : node) {
        ColDpMetadata.Agent agent = readAgent(child);
        if (agent != null) {
          sink.accept(agent);
        }
      }
      return;
    }

    ColDpMetadata.Agent agent = readAgent(node);
    if (agent != null) {
      sink.accept(agent);
    }
  }

  private ColDpMetadata.Agent readAgent(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }

    ColDpMetadata.Agent agent = new ColDpMetadata.Agent();
    if (node.isTextual()) {
      agent.setLiteral(asText(node));
      return agent;
    }
    if (!node.isObject()) {
      return null;
    }

    agent.setGiven(asText(node.get("given")));
    agent.setFamily(asText(node.get("family")));
    agent.setOrganization(asText(firstPresent(node, "organisation", "organization")));
    agent.setDepartment(asText(node.get("department")));
    agent.setCity(asText(node.get("city")));
    agent.setState(asText(node.get("state")));
    agent.setCountry(asText(node.get("country")));
    agent.setEmail(asText(node.get("email")));
    agent.setUrl(asText(firstPresent(node, "url", "path")));
    agent.setOrcid(asText(node.get("orcid")));
    agent.setRorid(asText(node.get("rorid")));
    agent.setNote(asText(node.get("note")));

    if (agent.getDisplayName() == null
        && agent.getEmail() == null
        && agent.getPrimaryOrganization() == null
        && agent.getOrcid() == null
        && agent.getRorid() == null) {
      return null;
    }
    return agent;
  }

  private JsonNode firstPresent(JsonNode node, String... fieldNames) {
    for (String fieldName : fieldNames) {
      JsonNode child = node.get(fieldName);
      if (child != null && !child.isNull()) {
        return child;
      }
    }
    return null;
  }

  private String asText(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String value = node.isValueNode() ? node.asText() : node.toString();
    if (value == null) {
      return null;
    }
    value = value.trim();
    return value.isEmpty() ? null : value;
  }

  private ZipEntry findEntry(ZipFile zipFile, List<String> acceptedNames) {
    return zipFile.stream()
        .filter(entry -> !entry.isDirectory())
        .filter(entry -> matchesEntry(entry.getName(), acceptedNames))
        .findFirst()
        .orElse(null);
  }

  private boolean matchesEntry(String entryName, List<String> acceptedNames) {
    String normalized = entryName.toLowerCase(Locale.ROOT);
    for (String acceptedName : acceptedNames) {
      String lowered = acceptedName.toLowerCase(Locale.ROOT);
      if (normalized.equals(lowered) || normalized.endsWith("/" + lowered)) {
        return true;
      }
    }
    return false;
  }
}
