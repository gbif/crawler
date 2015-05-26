package org.gbif.crawler.dwca.fragmenter;

import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.record.StarRecord;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StarRecordSerializerTest {

  @Test
  public void testIdenticalJson() throws IOException {
    String archiveDir1 = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");
    Archive archive1 = ArchiveFactory.openArchive(new File(archiveDir1));
    StarRecord record1 = archive1.iterator().next();
    DwcaTestUtil.cleanupArchive(archiveDir1);

    String archiveDir2 = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");
    Archive archive2 = ArchiveFactory.openArchive(new File(archiveDir2));
    StarRecord record2 = archive2.iterator().next();
    DwcaTestUtil.cleanupArchive(archiveDir2);

    byte[] json1 = StarRecordSerializer.toJson(record1);
    byte[] json2 = StarRecordSerializer.toJson(record2);
    assertTrue(Arrays.equals(json1, json2));
  }
}
