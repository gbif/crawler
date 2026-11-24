/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.dwcdp.downloader;

import org.gbif.crawler.common.HostRestrictions;
import org.gbif.crawler.common.SizeLimits;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DwcDpCrawlConsumerTest {

  @TempDir File archiveRepository;
  @TempDir File unpackedRepository;

  @Test
  public void testFailedUnpackLeavesNoTempDirectory() throws Exception {
    // No datapackage.json, so validation fails after unpacking
    File archive = zip("occurrence.csv");

    assertThrows(
        IOException.class,
        () -> consumer(new SizeLimits()).afterSuccessfulDownload(UUID.randomUUID(), null, archive));
    assertEquals(0, unpackedRepository.list().length);
  }

  private DwcDpCrawlConsumer consumer(SizeLimits limits) {
    return new DwcDpCrawlConsumer(
        null,
        null,
        archiveRepository,
        unpackedRepository,
        5_000,
        null,
        limits,
        new HostRestrictions());
  }

  private File zip(String... names) throws IOException {
    File archive = new File(archiveRepository, "archive.dwcdp");
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(archive.toPath()))) {
      for (String name : names) {
        out.putNextEntry(new ZipEntry(name));
        out.write("{}".getBytes());
        out.closeEntry();
      }
    }
    return archive;
  }
}
