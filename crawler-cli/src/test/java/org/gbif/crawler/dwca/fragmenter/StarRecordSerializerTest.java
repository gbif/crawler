package org.gbif.crawler.dwca.fragmenter;

import org.gbif.crawler.dwca.util.DwcaTestUtil;
import org.gbif.dwc.Archive;
import org.gbif.dwc.record.StarRecord;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StarRecordSerializerTest {

  @Test
  public void testIdenticalJson() throws IOException {
    Archive archive1 = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");
    StarRecord record1 = archive1.iterator().next();
    DwcaTestUtil.cleanupArchive(archive1);

    Archive archive2 = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");
    StarRecord record2 = archive2.iterator().next();
    DwcaTestUtil.cleanupArchive(archive2);

    byte[] json1 = StarRecordSerializer.toJson(record1);
    byte[] json2 = StarRecordSerializer.toJson(record2);
    assertTrue(Arrays.equals(json1, json2));
  }
}
