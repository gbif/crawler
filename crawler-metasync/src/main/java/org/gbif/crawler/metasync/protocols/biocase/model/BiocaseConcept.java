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
package org.gbif.crawler.metasync.protocols.biocase.model;

import org.apache.commons.digester3.annotations.rules.BeanPropertySetter;
import org.apache.commons.digester3.annotations.rules.ObjectCreate;
import org.apache.commons.digester3.annotations.rules.SetProperty;

import lombok.ToString;


@ObjectCreate(pattern = "response/content/capabilities/SupportedSchemas/Concept")
@ToString
public class BiocaseConcept {

  @SetProperty(
      pattern = "response/content/capabilities/SupportedSchemas/Concept",
      attributeName = "searchable")
  private boolean searchable = true;

  @SetProperty(
      pattern = "response/content/capabilities/SupportedSchemas/Concept",
      attributeName = "dataType")
  private String dataType;

  @BeanPropertySetter(pattern = "response/content/capabilities/SupportedSchemas/Concept")
  private String name;

  public boolean isSearchable() {
    return searchable;
  }

  public void setSearchable(boolean searchable) {
    this.searchable = searchable;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
