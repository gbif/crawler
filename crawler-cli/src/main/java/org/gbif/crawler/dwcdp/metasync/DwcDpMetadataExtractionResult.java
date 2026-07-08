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
package org.gbif.crawler.dwcdp.metasync;

import javax.annotation.Nullable;

/**
 * Result of extracting metadata from a DwcDP archive.
 *
 * <p>The {@code datapackageDocument} is always present (datapackage.json is mandatory in DwcDP).
 * The {@code emlDocument} is optional — it is populated when the archive contains an eml.xml file.
 * When both are present the EML document should be treated as the primary metadata by the registry,
 * while the datapackage document captures format-specific fields.
 */
public class DwcDpMetadataExtractionResult {

  private final DwcDpMetadataDocument datapackageDocument;
  private final DwcDpMetadataDocument emlDocument;

  public DwcDpMetadataExtractionResult(
      DwcDpMetadataDocument datapackageDocument, @Nullable DwcDpMetadataDocument emlDocument) {
    this.datapackageDocument = datapackageDocument;
    this.emlDocument = emlDocument;
  }

  public DwcDpMetadataDocument getDatapackageDocument() {
    return datapackageDocument;
  }

  @Nullable
  public DwcDpMetadataDocument getEmlDocument() {
    return emlDocument;
  }

  public boolean hasEml() {
    return emlDocument != null;
  }
}
