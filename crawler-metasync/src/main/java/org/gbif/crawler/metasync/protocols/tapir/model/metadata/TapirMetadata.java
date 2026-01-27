/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.metasync.protocols.tapir.model.metadata;

import org.gbif.api.vocabulary.Language;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.digester3.annotations.rules.BeanPropertySetter;
import org.apache.commons.digester3.annotations.rules.CallMethod;
import org.apache.commons.digester3.annotations.rules.CallParam;
import org.apache.commons.digester3.annotations.rules.ObjectCreate;
import org.apache.commons.digester3.annotations.rules.SetNext;
import org.apache.commons.digester3.annotations.rules.SetProperty;

import jakarta.validation.constraints.NotNull;
import lombok.ToString;

@ObjectCreate(pattern = "response")
@ToString
public class TapirMetadata {

  private final List<TapirRelatedEntity> relatedEntities = new ArrayList<>();
  // TODO: Validation
  private final LocalizedString titles = new LocalizedString();
  private final LocalizedString descriptions = new LocalizedString();
  private final Set<Language> languages = new HashSet<>();
  private final LocalizedString bibliographicCitations = new LocalizedString();
  private final LocalizedString rights = new LocalizedString();

  @SetProperty(pattern = "response/header/source/software", attributeName = "name")
  private String softwareName;

  @SetProperty(pattern = "response/header/source/software", attributeName = "version")
  private String softwareVersion;

  @SetProperty(pattern = "response/metadata", attributeName = "lang")
  private Language defaultLanguage;

  @BeanPropertySetter(pattern = "response/metadata/type")
  @NotNull
  private String type;

  @BeanPropertySetter(pattern = "response/metadata/accesspoint")
  @NotNull
  private URI accessPoint;

  private LocalizedString subjects = new LocalizedString();

  @BeanPropertySetter(pattern = "response/metadata/created")
  private OffsetDateTime created;

  private IndexingPreferences indexingPreferences;

  @BeanPropertySetter(pattern = "response/metadata/modified")
  private OffsetDateTime modified;

  @BeanPropertySetter(pattern = "response/metadata/custom/identifier")
  private String identifier;

  @CallMethod(pattern = "response/metadata/title")
  public void addTitle(
    @CallParam(pattern = "response/metadata/title", attributeName = "xml:lang") Language language,
    @CallParam(pattern = "response/metadata/title") String title) {
    titles.addValue(language, title);
  }

  @CallMethod(pattern = "response/metadata/description")
  public void addDescription(
    @CallParam(pattern = "response/metadata/description", attributeName = "xml:lang")
    Language language,
    @CallParam(pattern = "response/metadata/description") String description) {

    descriptions.addValue(language, description);
  }

  @CallMethod(pattern = "response/metadata/bibliographicCitation")
  public void addBibliographicCitation(
    @CallParam(pattern = "response/metadata/bibliographicCitation", attributeName = "xml:lang")
    Language language,
    @CallParam(pattern = "response/metadata/bibliographicCitation") String description) {

    bibliographicCitations.addValue(language, description);
  }

  @CallMethod(pattern = "response/metadata/language")
  public void addLanguage(@CallParam(pattern = "response/metadata/language") Language language) {
    languages.add(language);
  }

  @CallMethod(pattern = "response/metadata/rights")
  public void addRights(
    @CallParam(pattern = "response/metadata/rights", attributeName = "xml:lang")
    Language language,
    @CallParam(pattern = "response/metadata/rights") String description) {

    rights.addValue(language, description);
  }

  @CallMethod(pattern = "response/metadata/subject")
  public void addSubject(
    @CallParam(pattern = "response/metadata/subject", attributeName = "xml:lang")
    Language language,
    @CallParam(pattern = "response/metadata/subject") String description) {

    this.subjects.addValue(language, description);
  }

  @SetNext
  public void addRelatedEntity(TapirRelatedEntity contact) {
    relatedEntities.add(contact);
  }

  public List<TapirRelatedEntity> getRelatedEntities() {
    return relatedEntities;
  }

  public LocalizedString getTitles() {
    return titles;
  }

  public LocalizedString getDescriptions() {
    return descriptions;
  }

  public Set<Language> getLanguages() {
    return languages;
  }

  public LocalizedString getBibliographicCitations() {
    return bibliographicCitations;
  }

  public LocalizedString getRights() {
    return rights;
  }

  public String getSoftwareName() {
    return softwareName;
  }

  public void setSoftwareName(String softwareName) {
    this.softwareName = softwareName;
  }

  public String getSoftwareVersion() {
    return softwareVersion;
  }

  public void setSoftwareVersion(String softwareVersion) {
    this.softwareVersion = softwareVersion;
  }

  public Language getDefaultLanguage() {
    return defaultLanguage;
  }

  public void setDefaultLanguage(Language defaultLanguage) {
    this.defaultLanguage = defaultLanguage;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public URI getAccessPoint() {
    return accessPoint;
  }

  public void setAccessPoint(URI accessPoint) {
    this.accessPoint = accessPoint;
  }

  public LocalizedString getSubjects() {
    return subjects;
  }

  public void setSubjects(LocalizedString subjects) {
    this.subjects = subjects;
  }

  public OffsetDateTime getCreated() {
    return created;
  }

  public void setCreated(OffsetDateTime created) {
    this.created = created;
  }

  public IndexingPreferences getIndexingPreferences() {
    return indexingPreferences;
  }

  @SetNext
  public void setIndexingPreferences(IndexingPreferences indexingPreferences) {
    this.indexingPreferences = indexingPreferences;
  }

  public OffsetDateTime getModified() {
    return modified;
  }

  public void setModified(OffsetDateTime modified) {
    this.modified = modified;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }
}
