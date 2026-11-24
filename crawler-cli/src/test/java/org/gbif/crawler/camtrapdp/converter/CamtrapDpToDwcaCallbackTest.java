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
package org.gbif.crawler.camtrapdp.converter;

import org.gbif.crawler.common.SizeLimitExceededException;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CamtrapDpToDwcaCallbackTest {

  @TempDir File unpacked;

  @Test
  public void testDataPackageSizeLimit() throws Exception {
    Files.writeString(new File(unpacked, "datapackage.json").toPath(), "{\"name\":\"camtrap\"}");
    UUID key = UUID.randomUUID();

    assertDoesNotThrow(() -> CamtrapDpToDwcaCallback.validateUnpackedDp(key, unpacked, 100));
    assertDoesNotThrow(() -> CamtrapDpToDwcaCallback.validateUnpackedDp(key, unpacked, 0));
    assertThrows(
        SizeLimitExceededException.class,
        () -> CamtrapDpToDwcaCallback.validateUnpackedDp(key, unpacked, 10));
  }
}
