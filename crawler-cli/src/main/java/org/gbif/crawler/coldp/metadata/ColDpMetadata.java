package org.gbif.crawler.coldp.metadata;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * Minimal in-memory representation of the ColDP metadata fields the registry can consume.
 */
@Getter
@Setter
public class ColDpMetadata {

  private String title;
  private String description;
  private String doi;
  private String issued;
  private String version;
  private String license;
  private String url;
  private String logo;
  private String language;
  private String notes;
  private Map<String, String> identifiers;
  private final List<Agent> contacts = new ArrayList<>();
  private final List<Agent> creators = new ArrayList<>();
  private final List<Agent> editors = new ArrayList<>();
  private final List<Agent> contributors = new ArrayList<>();
  private Agent publisher;

  public boolean hasAnyMetadata() {
    return title != null
        || description != null
        || doi != null
        || url != null
        || logo != null
        || !contacts.isEmpty()
        || !creators.isEmpty()
        || !editors.isEmpty()
        || publisher != null;
  }

  @Setter
  @Getter
  public static class Agent {

    private String given;
    private String family;
    private String organization;
    private String department;
    private String city;
    private String state;
    private String country;
    private String email;
    private String url;
    private String orcid;
    private String rorid;
    private String note;
    private String literal;

    public String getDisplayName() {
      if (literal != null && !literal.isBlank()) {
        return literal;
      }
      StringBuilder name = new StringBuilder();
      if (given != null && !given.isBlank()) {
        name.append(given);
      }
      if (family != null && !family.isBlank()) {
        if (!name.isEmpty()) {
          name.append(' ');
        }
        name.append(family);
      }
      if (name.isEmpty() && organization != null && !organization.isBlank()) {
        name.append(organization);
      }
      return name.isEmpty() ? null : name.toString();
    }

    public String getPrimaryOrganization() {
      if (organization != null && !organization.isBlank()) {
        return organization;
      }
      return null;
    }

    public URI getParsedUrl() {
      try {
        return url == null || url.isBlank() ? null : URI.create(url);
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
  }
}
