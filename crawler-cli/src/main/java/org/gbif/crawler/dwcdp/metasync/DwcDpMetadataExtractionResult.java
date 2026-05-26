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
