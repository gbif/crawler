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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.digester3.annotations.rules.BeanPropertySetter;
import org.apache.commons.digester3.annotations.rules.ObjectCreate;
import org.apache.commons.digester3.annotations.rules.SetNext;

import lombok.ToString;

/** This is information about datasets retrieved using an inventory request as of BioCASe 3.4. */
@ObjectCreate(pattern = "inventory/datasets/dataset")
@ToString
public class InventoryDataset {

  @BeanPropertySetter(pattern = "inventory/datasets/dataset/title")
  private String title;

  @BeanPropertySetter(pattern = "inventory/datasets/dataset/id")
  private String id;

  private List<BiocaseArchive> archives = new ArrayList<>();

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<BiocaseArchive> getArchives() {
    return archives;
  }

  public void setArchives(List<BiocaseArchive> archives) {
    this.archives = archives;
  }

  @SetNext
  public void addArchive(BiocaseArchive archive) {
    archives.add(archive);
  }

}
