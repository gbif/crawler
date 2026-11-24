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

import org.gbif.utils.file.CompressionUtil;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;

import com.google.common.io.ByteStreams;

/**
 * Streams through an archive without writing anything, and fails if unpacking it would exceed the
 * {@link SizeLimits}. Formats are detected in the same order as {@link CompressionUtil}.
 */
public final class ArchiveSizeChecker {

  /** The compression ratio is only checked once this much has been unpacked. */
  static final long RATIO_THRESHOLD_BYTES = 1024L * 1024 * 1024;

  private static final int BUFFER = 64 * 1024;

  private final SizeLimits limits;
  private final long ratioThresholdBytes;
  private final long packedBytes;
  private long unpackedBytes;
  private int entries;

  private ArchiveSizeChecker(SizeLimits limits, long ratioThresholdBytes, long packedBytes) {
    this.limits = limits;
    this.ratioThresholdBytes = ratioThresholdBytes;
    this.packedBytes = Math.max(packedBytes, 1);
  }

  /**
   * @throws SizeLimitExceededException if unpacking the archive would exceed a limit
   */
  public static void check(File archive, SizeLimits limits) throws IOException {
    check(archive, limits, RATIO_THRESHOLD_BYTES);
  }

  static void check(File archive, SizeLimits limits, long ratioThresholdBytes)
      throws IOException {
    ArchiveSizeChecker checker =
        new ArchiveSizeChecker(limits, ratioThresholdBytes, archive.length());
    if (CompressionUtil.isZstdFormat(archive)) {
      checker.countTar(new ZstdCompressorInputStream(open(archive)));
    } else if (CompressionUtil.isGzipFormat(archive)) {
      try {
        checker.countTar(new GZIPInputStream(open(archive)));
      } catch (SizeLimitExceededException e) {
        throw e;
      } catch (IOException e) {
        // Not a tarball: a single gzipped file
        checker = new ArchiveSizeChecker(limits, ratioThresholdBytes, archive.length());
        checker.countEntry(new GZIPInputStream(open(archive)));
      }
    } else if (CompressionUtil.isTarFormat(archive)) {
      checker.countTar(open(archive));
    } else {
      checker.countZip(open(archive));
    }
  }

  /**
   * Reads a whole stream, such as a metadata file inside an archive.
   *
   * @param maxBytes maximum size, 0 for no limit
   * @throws SizeLimitExceededException if the stream is larger than maxBytes
   */
  public static byte[] readAllBytes(InputStream in, long maxBytes, String name)
      throws IOException {
    if (maxBytes <= 0 || maxBytes == Long.MAX_VALUE) {
      return in.readAllBytes();
    }
    byte[] bytes = ByteStreams.toByteArray(ByteStreams.limit(in, maxBytes + 1));
    if (bytes.length > maxBytes) {
      throw new SizeLimitExceededException(name + " is larger than " + maxBytes + " bytes");
    }
    return bytes;
  }

  private static InputStream open(File file) throws IOException {
    return new BufferedInputStream(Files.newInputStream(file.toPath()), BUFFER);
  }

  private void countTar(InputStream in) throws IOException {
    try (TarArchiveInputStream tar = new TarArchiveInputStream(in)) {
      while (tar.getNextTarEntry() != null) {
        addEntry();
        count(tar);
      }
    }
  }

  private void countZip(InputStream in) throws IOException {
    try (ZipInputStream zip = new ZipInputStream(in)) {
      while (zip.getNextEntry() != null) {
        addEntry();
        count(zip);
      }
    }
  }

  private void countEntry(InputStream in) throws IOException {
    try (InputStream stream = in) {
      addEntry();
      count(stream);
    }
  }

  private void addEntry() throws SizeLimitExceededException {
    entries++;
    if (limits.maxUnpackedEntries > 0 && entries > limits.maxUnpackedEntries) {
      throw new SizeLimitExceededException(
          "Archive has more than " + limits.maxUnpackedEntries + " entries");
    }
  }

  private void count(InputStream in) throws IOException {
    byte[] buffer = new byte[BUFFER];
    int read;
    while ((read = in.read(buffer)) != -1) {
      unpackedBytes += read;
      if (limits.maxUnpackedBytes > 0 && unpackedBytes > limits.maxUnpackedBytes) {
        throw new SizeLimitExceededException(
            "Archive unpacks to more than " + limits.maxUnpackedBytes + " bytes");
      }
      if (limits.maxCompressionRatio > 0
          && unpackedBytes > ratioThresholdBytes
          && unpackedBytes / packedBytes > limits.maxCompressionRatio) {
        throw new SizeLimitExceededException(
            "Archive compression ratio is more than " + limits.maxCompressionRatio);
      }
    }
  }
}
