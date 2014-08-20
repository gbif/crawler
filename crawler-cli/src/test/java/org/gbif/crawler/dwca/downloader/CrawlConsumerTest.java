package org.gbif.crawler.dwca.downloader;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import junit.framework.TestCase;

//@Ignore("Manual tests to run downloads that fail for unknown reasons")
public class CrawlConsumerTest extends TestCase {
  // please adapt to personal needs when running the tests manually!
  final static File DWCA_REPO = new File("/Users/markus/crawl-storage/dwca");

  public void testEbi() throws Exception {
    CrawlConsumer cc = new CrawlConsumer(null, null, DWCA_REPO);
    CrawlJob ebi = new CrawlJob(UUID.fromString("f6978ea9-5496-4efe-a874-364dddfaaa47"), 1, EndpointType.DWC_ARCHIVE,
                                URI.create("http://ftp.ebi.ac.uk/pub/databases/ena/biodiversity/occurrences/occurrences.tar.gz"));
    cc.doCrawl(ebi);
  }


  public void testEbi2() throws Exception {
    File ebi = new File(DWCA_REPO, "f6978ea9-5496-4efe-a874-364dddfaaa47.dwca");
    File dir = new File(DWCA_REPO, "f6978ea9-5496-4efe-a874-364dddfaaa47");
    if (dir.exists()) {
      // clean up any existing folder
      FileUtils.deleteDirectoryRecursively(dir);
    }
    org.apache.commons.io.FileUtils.forceMkdir(dir);

    CompressionUtil.decompressFile(dir, ebi, true);
    ArchiveFactory.openArchive(dir);
  }

}