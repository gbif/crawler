package org.gbif.crawler.dwca;

import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ArchiveFactoryTest {

  @Test
  public void testNoSlash() throws IOException {
    String archiveDir = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");

    Archive archive = ArchiveFactory.openArchive(new File(archiveDir));
    assertNotNull(archive);

    DwcaTestUtil.cleanupArchive(archiveDir);
  }

  @Test
  public void testWithSlash() throws IOException {
    String archiveDir = DwcaTestUtil.openArchive("/dwca/dwca-with-backslashes.zip");

    Archive archive = ArchiveFactory.openArchive(new File(archiveDir));
    assertNotNull(archive);

    DwcaTestUtil.cleanupArchive(archiveDir);
  }
}
