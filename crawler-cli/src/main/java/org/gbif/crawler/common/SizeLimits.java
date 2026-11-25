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

import com.beust.jcommander.Parameter;

import jakarta.validation.constraints.PositiveOrZero;
import lombok.ToString;

/**
 * Limits applied to content fetched from publishers, so a single dataset can't exhaust the disk or
 * memory of a crawler. A value of 0 disables that limit.
 */
@ToString
public class SizeLimits {

  private static final long MiB = 1024L * 1024;
  private static final long GiB = 1024 * MiB;

  @Parameter(
      names = "--max-download-bytes",
      description = "Maximum size of a downloaded archive in bytes, 0 for no limit")
  @PositiveOrZero
  public long maxDownloadBytes = 200 * GiB;

  @Parameter(
      names = "--max-unpacked-bytes",
      description = "Maximum total size of an archive once unpacked in bytes, 0 for no limit")
  @PositiveOrZero
  public long maxUnpackedBytes = 1024 * GiB;

  @Parameter(
      names = "--max-unpacked-entries",
      description = "Maximum number of files in an archive, 0 for no limit")
  @PositiveOrZero
  public int maxUnpackedEntries = 10_000;

  @Parameter(
      names = "--max-compression-ratio",
      description =
          "Maximum ratio of unpacked to packed size, checked once more than 1 GiB is unpacked, "
              + "0 for no limit")
  @PositiveOrZero
  public int maxCompressionRatio = 200;

  @Parameter(
      names = "--max-metadata-bytes",
      description = "Maximum size of a metadata file read from an archive in bytes, 0 for no limit")
  @PositiveOrZero
  public long maxMetadataBytes = 50 * MiB;
}
