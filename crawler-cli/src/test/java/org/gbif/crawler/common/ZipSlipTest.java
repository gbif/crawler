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

import org.gbif.dwc.DwcFiles;
import org.gbif.utils.file.CompressionUtil;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Hostile archives through the extraction calls the crawler uses (gbif-common CompressionUtil and
 * dwca-io DwcFiles) must not write outside the target directory.
 */
public class ZipSlipTest {

  private static final List<String> HOSTILE =
      List.of("../evil1.txt", "a/../../evil2.txt", "./../evil3.txt", "a/b/../../../evil4.txt");

  @TempDir File parent;

  @ParameterizedTest
  @ValueSource(strings = {"zip", "tgz"})
  public void testNothingWrittenOutsideTarget(String format) throws Exception {
    File archive = new File(parent, "archive." + format);
    if ("zip".equals(format)) {
      writeZip(archive);
    } else {
      writeTgz(archive);
    }

    for (boolean keepSubdirectories : new boolean[] {true, false}) {
      File target = new File(parent, "target-" + keepSubdirectories);
      target.mkdir();
      try {
        CompressionUtil.decompressFile(target, archive, keepSubdirectories);
      } catch (Exception e) {
        // failing is fine, writing outside is not
      }
    }
    try {
      DwcFiles.fromCompressed(archive.toPath(), new File(parent, "dwca").toPath());
    } catch (Exception e) {
      // not a valid DwC-A
    }

    assertEquals(List.of(), outsideFiles());
  }

  /** Targets are subdirectories of parent, so escaped files land in parent or above it. */
  private List<String> outsideFiles() throws IOException {
    try (Stream<Path> here = Files.list(parent.toPath());
        Stream<Path> above = Files.list(parent.getParentFile().toPath())) {
      return Stream.concat(here, above)
          .map(p -> p.getFileName().toString())
          .filter(n -> n.startsWith("evil"))
          .toList();
    }
  }

  /** Checks the test can see an escape: a naive extraction of the same zip writes outside. */
  @Test
  public void testDetectorSeesNaiveExtraction() throws Exception {
    File archive = new File(parent, "archive.zip");
    writeZip(archive);
    File target = new File(parent, "naive");
    target.mkdir();
    try (ZipInputStream in = new ZipInputStream(Files.newInputStream(archive.toPath()))) {
      ZipEntry entry;
      while ((entry = in.getNextEntry()) != null) {
        // The classic vulnerable pattern: resolve without checking the result stays inside
        Path out = target.toPath().resolve(entry.getName()).normalize();
        Files.createDirectories(out.getParent());
        Files.copy(in, out);
      }
    }
    // 4 relative escapes and the absolute entry
    assertEquals(5, outsideFiles().size());
    for (String name : outsideFiles()) {
      new File(parent, name).delete();
    }
  }

  private void writeZip(File zip) throws IOException {
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip.toPath()))) {
      for (String name : HOSTILE) {
        out.putNextEntry(new ZipEntry(name));
        out.write("evil".getBytes());
        out.closeEntry();
      }
      // Absolute path, written under the target directory by gbif-common 1.1
      out.putNextEntry(new ZipEntry(new File(parent, "evil5.txt").getAbsolutePath()));
      out.write("evil".getBytes());
      out.closeEntry();
    }
  }

  private void writeTgz(File tgz) throws IOException {
    try (TarArchiveOutputStream out =
        new TarArchiveOutputStream(new GZIPOutputStream(Files.newOutputStream(tgz.toPath())))) {
      for (String name : HOSTILE) {
        TarArchiveEntry entry = new TarArchiveEntry(name, true);
        entry.setSize(4);
        out.putArchiveEntry(entry);
        out.write("evil".getBytes());
        out.closeArchiveEntry();
      }
    }
  }
}
