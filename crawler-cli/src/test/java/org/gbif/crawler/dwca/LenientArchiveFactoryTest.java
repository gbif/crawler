package org.gbif.crawler.dwca;

import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwc.text.Archive;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class LenientArchiveFactoryTest {

  @Test
  public void testNoSlash() throws IOException {
    String archiveDir = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");

    Archive archive = LenientArchiveFactory.openArchive(new File(archiveDir));
    assertNotNull(archive);

    DwcaTestUtil.cleanupArchive(archiveDir);
  }

  @Test
  public void testWithSlash() throws IOException {
    String archiveDir = DwcaTestUtil.openArchive("/dwca/dwca-with-backslashes.zip");

    Archive archive = LenientArchiveFactory.openArchive(new File(archiveDir));
    assertNotNull(archive);

    DwcaTestUtil.cleanupArchive(archiveDir);
  }
}
