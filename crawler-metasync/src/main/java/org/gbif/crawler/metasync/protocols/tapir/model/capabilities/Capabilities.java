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
package org.gbif.crawler.metasync.protocols.tapir.model.capabilities;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.digester3.annotations.rules.BeanPropertySetter;
import org.apache.commons.digester3.annotations.rules.ObjectCreate;
import org.apache.commons.digester3.annotations.rules.SetNext;

import lombok.ToString;

@ObjectCreate(pattern = "response/capabilities")
@ToString
public class Capabilities {

  @BeanPropertySetter(pattern = "response/capabilities/settings/minQueryTermLength")
  private int minQueryTermLength;

  @BeanPropertySetter(pattern = "response/capabilities/settings/maxElementRepetitions")
  private int maxElementRepetitions;

  @BeanPropertySetter(pattern = "response/capabilities/settings/maxElementLevels")
  private int maxElementLevels;

  @BeanPropertySetter(pattern = "response/capabilities/settings/maxResponseTags")
  private int maxResponseTags;

  @BeanPropertySetter(pattern = "response/capabilities/settings/maxResponseSize")
  private int maxResponseSize;

  private final List<Schema> schemas = new ArrayList<>();

  private final List<Archive> archives = new ArrayList<>();

  public int getMinQueryTermLength() {
    return minQueryTermLength;
  }

  public void setMinQueryTermLength(int minQueryTermLength) {
    this.minQueryTermLength = minQueryTermLength;
  }

  public int getMaxElementRepetitions() {
    return maxElementRepetitions;
  }

  public void setMaxElementRepetitions(int maxElementRepetitions) {
    this.maxElementRepetitions = maxElementRepetitions;
  }

  public int getMaxElementLevels() {
    return maxElementLevels;
  }

  public void setMaxElementLevels(int maxElementLevels) {
    this.maxElementLevels = maxElementLevels;
  }

  public int getMaxResponseTags() {
    return maxResponseTags;
  }

  public void setMaxResponseTags(int maxResponseTags) {
    this.maxResponseTags = maxResponseTags;
  }

  public int getMaxResponseSize() {
    return maxResponseSize;
  }

  public void setMaxResponseSize(int maxResponseSize) {
    this.maxResponseSize = maxResponseSize;
  }

  public List<Schema> getSchemas() {
    return schemas;
  }

  public List<Archive> getArchives() {
    return archives;
  }

  @SetNext
  public void addSchema(Schema schema) {
    schemas.add(schema);
  }

  @SetNext
  public void addArchive(Archive archive) {
    archives.add(archive);
  }

}
