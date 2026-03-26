package org.gbif.crawler.coldp.metadata;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ColDpMetadataParserTest {

  private final ColDpMetadataParser parser = new ColDpMetadataParser();

  @Test
  void parsesCurrentYamlShape() throws Exception {
    String yaml =
        """
        title: Sample Checklist
        description: Example description
        doi: 10.15468/2zjeva
        issued: 2026-01-13
        version: v1
        license: CC0-1.0
        url: https://example.org/checklist
        logo: https://example.org/logo.png
        identifier:
          col: 1010
          gbif: abc-123
        contact:
          given: Jane
          family: Doe
          email: jane@example.org
        creator:
          - given: Alice
            family: Author
            orcid: 0000-0000-0000-0001
          - Example Organisation
        editor:
          given: Ed
          family: Itor
        publisher:
          organisation: GBIF
          rorid: https://ror.org/00x0x0000
        notes: Additional notes
        """;

    ColDpMetadata metadata =
        parser.parseYaml(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

    assertEquals("Sample Checklist", metadata.getTitle());
    assertEquals("Example description", metadata.getDescription());
    assertEquals("10.15468/2zjeva", metadata.getDoi());
    assertEquals("CC0-1.0", metadata.getLicense());
    assertEquals("https://example.org/checklist", metadata.getUrl());
    assertEquals(2, metadata.getIdentifiers().size());
    assertEquals(1, metadata.getContacts().size());
    assertEquals("Jane Doe", metadata.getContacts().get(0).getDisplayName());
    assertEquals(2, metadata.getCreators().size());
    assertEquals("Alice Author", metadata.getCreators().get(0).getDisplayName());
    assertEquals("Example Organisation", metadata.getCreators().get(1).getDisplayName());
    assertNotNull(metadata.getPublisher());
    assertEquals("GBIF", metadata.getPublisher().getPrimaryOrganization());
  }
}
