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
package org.gbif.crawler.metasync.protocols.digir.model;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.digester3.annotations.rules.BeanPropertySetter;
import org.apache.commons.digester3.annotations.rules.CallMethod;
import org.apache.commons.digester3.annotations.rules.CallParam;
import org.apache.commons.digester3.annotations.rules.ObjectCreate;
import org.apache.commons.digester3.annotations.rules.SetNext;

import lombok.ToString;

@ObjectCreate(pattern = "response/content/metadata/provider/host")
@ToString
public class DigirHost {

  private final Set<URI> relatedInformation = new HashSet<>();

  @BeanPropertySetter(pattern = "response/content/metadata/provider/host/name")
  private String name;

  @BeanPropertySetter(pattern = "response/content/metadata/provider/host/code")
  private String code;
  /** Called {@code abstract} in DiGIR, which is a reserved word in Java. */
  @BeanPropertySetter(pattern = "response/content/metadata/provider/host/abstract")
  private String description;

  private final List<DigirContact> contacts = new ArrayList<>();

  @CallMethod(pattern = "response/content/metadata/provider/host/relatedInformation")
  public void addRelatedInformation(
      @CallParam(pattern = "response/content/metadata/provider/host/relatedInformation")
          URI relatedInformation) {
    this.relatedInformation.add(relatedInformation);
  }

  public Set<URI> getRelatedInformation() {
    return relatedInformation;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @SetNext
  public void addContact(DigirHostContact contact) {
    contacts.add(contact);
  }

  public List<DigirContact> getContacts() {
    return contacts;
  }

}
