package org.gbif.crawler.coldp.metasync;

import org.gbif.api.vocabulary.MetadataType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Raw metadata document extracted from a COLDP archive plus an optional normalized JSON companion.
 */
public class ColDpMetadataDocument {

  private final byte[] rawDocument;
  private final String contentJson;
  private final MetadataType metadataType;

  public ColDpMetadataDocument(byte[] rawDocument, String contentJson, MetadataType metadataType) {
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
