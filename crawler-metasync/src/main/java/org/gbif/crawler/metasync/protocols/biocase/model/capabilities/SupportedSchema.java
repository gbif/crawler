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
package org.gbif.crawler.metasync.protocols.biocase.model.capabilities;

import org.gbif.crawler.metasync.protocols.biocase.model.BiocaseConcept;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.digester3.annotations.rules.ObjectCreate;
import org.apache.commons.digester3.annotations.rules.SetNext;
import org.apache.commons.digester3.annotations.rules.SetProperty;

import lombok.ToString;

@ObjectCreate(pattern = "response/content/capabilities/SupportedSchemas")
@ToString
public class SupportedSchema {

  @SetProperty(
      pattern = "response/content/capabilities/SupportedSchemas",
      attributeName = "namespace")
  private URI namespace;

  @SetProperty(
      pattern = "response/content/capabilities/SupportedSchemas",
      attributeName = "request")
  private boolean request;

  @SetProperty(
      pattern = "response/content/capabilities/SupportedSchemas",
      attributeName = "response")
  private boolean response;

  private List<BiocaseConcept> concepts = new ArrayList<>();

  public URI getNamespace() {
    return namespace;
  }

  public void setNamespace(URI namespace) {
    this.namespace = namespace;
  }

  public boolean isRequest() {
    return request;
  }

  public void setRequest(boolean request) {
    this.request = request;
  }

  public boolean isResponse() {
    return response;
  }

  public void setResponse(boolean response) {
    this.response = response;
  }

  public List<BiocaseConcept> getConcepts() {
    return concepts;
  }

  public void setConcepts(List<BiocaseConcept> concepts) {
    this.concepts = concepts;
  }

  @SetNext
  public void addConcept(BiocaseConcept concept) {
    concepts.add(concept);
  }

}
