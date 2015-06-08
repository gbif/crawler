package org.gbif.crawler.dwca;

import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwca.io.Archive;

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ArchiveFactoryTest {

  @Test
  public void testNoSlash() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");
    assertNotNull(archive);
    DwcaTestUtil.cleanupArchive(archive);
  }

  @Test
  public void testWithSlash() throws IOException {
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-with-backslashes.zip");
    assertNotNull(archive);
    DwcaTestUtil.cleanupArchive(archive);
  }
}
