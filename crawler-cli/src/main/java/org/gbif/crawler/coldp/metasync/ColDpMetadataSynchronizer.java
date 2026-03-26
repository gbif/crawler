package org.gbif.crawler.coldp.metasync;

import org.gbif.api.model.common.DOI;
import org.gbif.api.model.registry.Citation;
import org.gbif.api.model.registry.Contact;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.ContactType;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.Language;
import org.gbif.api.vocabulary.License;
import org.gbif.crawler.coldp.metadata.ColDpMetadata;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies parsed ColDP metadata to an existing registry dataset.
 */
public class ColDpMetadataSynchronizer {

  private static final Logger LOG = LoggerFactory.getLogger(ColDpMetadataSynchronizer.class);

  private final DatasetService datasetService;

  public ColDpMetadataSynchronizer(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  public void synchronize(UUID datasetKey, Dataset dataset, ColDpMetadata metadata) {
    if (metadata.getTitle() != null) {
      dataset.setTitle(metadata.getTitle());
    }

    String description = buildDescription(metadata);
    if (description != null) {
      dataset.setDescription(description);
    }

    URI homepage = parseUri(metadata.getUrl());
    if (homepage != null) {
      dataset.setHomepage(homepage);
    }

    URI logo = parseUri(metadata.getLogo());
    if (logo != null) {
      dataset.setLogoUrl(logo);
    }

    Language language = parseLanguage(metadata.getLanguage());
    if (language != null && language != Language.UNKNOWN) {
      dataset.setLanguage(language);
    }

    dataset.setRights(null);

    License license = parseLicense(metadata.getLicense());
    if (license != null && license.isConcrete()) {
      dataset.setLicense(license);
    }

    Citation citation = buildCitation(metadata);
    if (citation != null) {
      dataset.setCitation(citation);
    }

    applyPrimaryIdentifier(dataset, metadata);
    datasetService.update(dataset);
    replaceContacts(datasetKey, dataset, metadata);
    addIdentifiers(datasetKey, dataset, metadata.getIdentifiers());
  }

  private void replaceContacts(UUID datasetKey, Dataset dataset, ColDpMetadata metadata) {
    List<Contact> replacementContacts = new ArrayList<>();
    replacementContacts.addAll(toContacts(metadata.getContacts(), ContactType.POINT_OF_CONTACT));
    replacementContacts.addAll(toContacts(metadata.getCreators(), ContactType.ORIGINATOR));
    replacementContacts.addAll(toContacts(metadata.getEditors(), ContactType.ADMINISTRATIVE_POINT_OF_CONTACT));
    replacementContacts.addAll(toContacts(metadata.getContributors(), ContactType.CONTENT_PROVIDER));

    if (metadata.getPublisher() != null) {
      Contact publisherContact = toContact(metadata.getPublisher(), ContactType.DISTRIBUTOR);
      if (publisherContact != null) {
        replacementContacts.add(publisherContact);
      }
    }

    if (dataset.getContacts() != null) {
      for (Contact contact : dataset.getContacts()) {
        datasetService.deleteContact(datasetKey, contact.getKey());
      }
    }
    for (Contact contact : replacementContacts) {
      datasetService.addContact(datasetKey, contact);
    }
  }

  private void addIdentifiers(UUID datasetKey, Dataset dataset, Map<String, String> identifiers) {
    if (identifiers == null || identifiers.isEmpty()) {
      return;
    }

    List<Identifier> existingIdentifiers =
        dataset.getIdentifiers() == null ? List.of() : dataset.getIdentifiers();

    for (Map.Entry<String, String> entry : identifiers.entrySet()) {
      Identifier identifier = new Identifier();
      identifier.setType(IdentifierType.UNKNOWN);
      identifier.setIdentifier(entry.getKey() + ":" + entry.getValue());
      if (existingIdentifiers.stream().noneMatch(identifier::lenientEquals)) {
        datasetService.addIdentifier(datasetKey, identifier);
      }
    }
  }

  private void applyPrimaryIdentifier(Dataset dataset, ColDpMetadata metadata) {
    if (StringUtils.isBlank(metadata.getDoi())) {
      return;
    }

    if (DOI.isParsable(metadata.getDoi())) {
      dataset.setDoi(new DOI(metadata.getDoi()));
    } else {
      LOG.warn("Ignoring non-parsable DOI value {}", metadata.getDoi());
    }
  }

  private List<Contact> toContacts(List<ColDpMetadata.Agent> agents, ContactType type) {
    List<Contact> contacts = new ArrayList<>();
    for (ColDpMetadata.Agent agent : agents) {
      Contact contact = toContact(agent, type);
      if (contact != null) {
        contacts.add(contact);
      }
    }
    return contacts;
  }

  private Contact toContact(ColDpMetadata.Agent agent, ContactType type) {
    if (agent == null) {
      return null;
    }

    Contact contact = new Contact();
    contact.setType(type);

    if (agent.getLiteral() != null) {
      contact.setLastName(agent.getLiteral());
    } else {
      contact.setFirstName(agent.getGiven());
      contact.setLastName(agent.getFamily());
    }
    contact.setOrganization(agent.getPrimaryOrganization());
    if (agent.getEmail() != null) {
      contact.addEmail(agent.getEmail());
    }
    if (agent.getParsedUrl() != null) {
      contact.addHomepage(agent.getParsedUrl());
    }
    if (agent.getOrcid() != null) {
      contact.addUserId(agent.getOrcid());
    }
    if (agent.getRorid() != null) {
      contact.addUserId(agent.getRorid());
    }
    if (agent.getDepartment() != null) {
      contact.addPosition(agent.getDepartment());
    }
    if (agent.getNote() != null) {
      contact.setDescription(agent.getNote());
    }

    if (agent.getDisplayName() == null
        && agent.getPrimaryOrganization() == null
        && agent.getEmail() == null
        && agent.getOrcid() == null
        && agent.getRorid() == null) {
      return null;
    }
    return contact;
  }

  private Citation buildCitation(ColDpMetadata metadata) {
    if (StringUtils.isBlank(metadata.getTitle())) {
      return null;
    }

    List<String> pieces = new ArrayList<>();
    String creators = joinAgentNames(metadata.getCreators());
    if (creators != null) {
      pieces.add(creators);
    }
    if (metadata.getIssued() != null) {
      pieces.add("(" + metadata.getIssued() + ")");
    }
    pieces.add(metadata.getTitle());
    if (metadata.getVersion() != null) {
      pieces.add(metadata.getVersion());
    }
    if (metadata.getPublisher() != null && metadata.getPublisher().getDisplayName() != null) {
      pieces.add(metadata.getPublisher().getDisplayName());
    }
    return new Citation(String.join(". ", pieces), null, false);
  }

  private String buildDescription(ColDpMetadata metadata) {
    if (metadata.getDescription() == null) {
      return metadata.getNotes();
    }
    if (metadata.getNotes() == null) {
      return metadata.getDescription();
    }
    return metadata.getDescription() + "\n\n" + metadata.getNotes();
  }

  private String joinAgentNames(List<ColDpMetadata.Agent> agents) {
    List<String> names = new ArrayList<>();
    for (ColDpMetadata.Agent agent : agents) {
      if (agent.getDisplayName() != null) {
        names.add(agent.getDisplayName());
      }
    }
    return names.isEmpty() ? null : String.join("; ", names);
  }

  private URI parseUri(String value) {
    try {
      return StringUtils.isBlank(value) ? null : URI.create(value);
    } catch (IllegalArgumentException e) {
      LOG.warn("Ignoring invalid URI {}", value);
      return null;
    }
  }

  private License parseLicense(String value) {
    return StringUtils.isBlank(value) ? null : License.fromString(value).orElse(null);
  }

  private Language parseLanguage(String value) {
    return StringUtils.isBlank(value) ? null : Language.fromIsoCode(value);
  }
}
