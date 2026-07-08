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

import org.gbif.api.vocabulary.MetadataType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Raw metadata document extracted from a DwcDP archive plus an optional normalized JSON companion.
 */
public class DwcDpMetadataDocument {

  private final byte[] rawDocument;
  private final String contentJson;
  private final MetadataType metadataType;

  public DwcDpMetadataDocument(byte[] rawDocument, String contentJson, MetadataType metadataType) {
    this.rawDocument = rawDocument;
    this.contentJson = contentJson;
    this.metadataType = metadataType;
  }

  public InputStream rawDocumentStream() {
    return new ByteArrayInputStream(rawDocument);
  }

  public String getContentJson() {
    return contentJson;
  }

  public MetadataType getMetadataType() {
    return metadataType;
  }
}
