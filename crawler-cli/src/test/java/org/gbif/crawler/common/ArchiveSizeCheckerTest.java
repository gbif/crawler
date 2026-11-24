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
package org.gbif.crawler.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ArchiveSizeCheckerTest {

  @TempDir Path tempDir;

  @Test
  public void testZipWithinLimits() throws Exception {
    File zip = zip(3, 1_000);
    assertDoesNotThrow(() -> ArchiveSizeChecker.check(zip, limits(10_000, 3, 0)));
  }

  @Test
  public void testZipTooLarge() throws Exception {
    File zip = zip(3, 1_000);
    assertThrows(
        SizeLimitExceededException.class,
        () -> ArchiveSizeChecker.check(zip, limits(2_999, 0, 0)));
  }

  @Test
  public void testZipTooManyEntries() throws Exception {
    File zip = zip(4, 10);
    assertThrows(
        SizeLimitExceededException.class, () -> ArchiveSizeChecker.check(zip, limits(0, 3, 0)));
  }

  @Test
  public void testCompressionRatio() throws Exception {
    // 1 MB of zeros compresses to about 1 KB
    File zip = zip(1, 1_000_000);
    assertThrows(
        SizeLimitExceededException.class,
        () -> ArchiveSizeChecker.check(zip, limits(0, 0, 100), 10_000));
    assertDoesNotThrow(() -> ArchiveSizeChecker.check(zip, limits(0, 0, 100), 2_000_000));
  }

  @Test
  public void testTarGzTooLarge() throws Exception {
    File tgz = tempDir.resolve("archive.tar.gz").toFile();
    try (TarArchiveOutputStream tar =
        new TarArchiveOutputStream(new GZIPOutputStream(Files.newOutputStream(tgz.toPath())))) {
      for (int i = 0; i < 2; i++) {
        TarArchiveEntry entry = new TarArchiveEntry("file" + i + ".txt");
        entry.setSize(1_000);
        tar.putArchiveEntry(entry);
        tar.write(new byte[1_000]);
        tar.closeArchiveEntry();
      }
    }
    assertDoesNotThrow(() -> ArchiveSizeChecker.check(tgz, limits(2_000, 0, 0)));
    assertThrows(
        SizeLimitExceededException.class,
        () -> ArchiveSizeChecker.check(tgz, limits(1_999, 0, 0)));
  }

  @Test
  public void testGzipTooLarge() throws Exception {
    File gz = tempDir.resolve("eml.xml.gz").toFile();
    try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(gz.toPath()))) {
      out.write("<eml>".repeat(1_000).getBytes());
    }
    assertDoesNotThrow(() -> ArchiveSizeChecker.check(gz, limits(5_000, 0, 0)));
    assertThrows(
        SizeLimitExceededException.class, () -> ArchiveSizeChecker.check(gz, limits(4_999, 0, 0)));
  }

  @Test
  public void testReadAllBytes() throws Exception {
    byte[] data = new byte[100];
    assertArrayEquals(
        data, ArchiveSizeChecker.readAllBytes(new ByteArrayInputStream(data), 100, "eml.xml"));
    assertArrayEquals(
        data, ArchiveSizeChecker.readAllBytes(new ByteArrayInputStream(data), 0, "eml.xml"));
    assertThrows(
        SizeLimitExceededException.class,
        () -> ArchiveSizeChecker.readAllBytes(new ByteArrayInputStream(data), 99, "eml.xml"));
  }

  private File zip(int entries, int entrySize) throws IOException {
    File zip = tempDir.resolve("archive" + entries + "-" + entrySize + ".zip").toFile();
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip.toPath()))) {
      for (int i = 0; i < entries; i++) {
        out.putNextEntry(new ZipEntry("file" + i + ".txt"));
        out.write(new byte[entrySize]);
        out.closeEntry();
      }
    }
    return zip;
  }

  private static SizeLimits limits(long maxUnpackedBytes, int maxEntries, int maxRatio) {
    SizeLimits limits = new SizeLimits();
    limits.maxUnpackedBytes = maxUnpackedBytes;
    limits.maxUnpackedEntries = maxEntries;
    limits.maxCompressionRatio = maxRatio;
    return limits;
  }
}
