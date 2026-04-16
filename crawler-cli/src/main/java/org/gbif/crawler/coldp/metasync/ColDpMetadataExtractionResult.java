package org.gbif.crawler.coldp.metasync;

import javax.annotation.Nullable;

/**
 * Result of extracting metadata from a COLDP archive.
 *
 * <p>The {@code formatDocument} is always present (metadata.yaml/json is the primary COLDP
 * metadata file). The {@code emlDocument} is optional — it is populated when the archive contains a
 * eml.xml (EML) file. When both are present the EML document should be treated as the primary
 * metadata by the registry, while the format document captures ColDP-specific fields.
 */
public class ColDpMetadataExtractionResult {

  private final ColDpMetadataDocument formatDocument;
  private final ColDpMetadataDocument emlDocument;

  public ColDpMetadataExtractionResult(
      ColDpMetadataDocument formatDocument, @Nullable ColDpMetadataDocument emlDocument) {
    this.formatDocument = formatDocument;
    this.emlDocument = emlDocument;
  }

  public ColDpMetadataDocument getFormatDocument() {
    return formatDocument;
  }

  @Nullable
  public ColDpMetadataDocument getEmlDocument() {
    return emlDocument;
  }

  public boolean hasEml() {
    return emlDocument != null;
  }
}
