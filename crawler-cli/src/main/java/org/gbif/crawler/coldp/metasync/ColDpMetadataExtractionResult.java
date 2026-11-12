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
package org.gbif.crawler.coldp.metasync;

import javax.annotation.Nullable;

/**
 * Result of extracting metadata from a COLDP archive.
 *
 * <p>Both documents are optional. The {@code formatDocument} is populated when the archive contains
 * a metadata.yaml/json file, the {@code emlDocument} when it contains an eml.xml (EML) file. When
 * both are present the EML document should be treated as the primary metadata by the registry, while the format document captures ColDP-specific fields.
 */
public class ColDpMetadataExtractionResult {

  private final ColDpMetadataDocument formatDocument;
  private final ColDpMetadataDocument emlDocument;

  public ColDpMetadataExtractionResult(
      @Nullable ColDpMetadataDocument formatDocument,
      @Nullable ColDpMetadataDocument emlDocument) {
    this.formatDocument = formatDocument;
    this.emlDocument = emlDocument;
  }

  @Nullable
  public ColDpMetadataDocument getFormatDocument() {
    return formatDocument;
  }

  /** The format document as JSON, or null when the archive has no metadata.yaml/json. */
  @Nullable
  public String getFormatContentJson() {
    return formatDocument != null ? formatDocument.getContentJson() : null;
  }

  @Nullable
  public ColDpMetadataDocument getEmlDocument() {
    return emlDocument;
  }

  public boolean hasEml() {
    return emlDocument != null;
  }
}
