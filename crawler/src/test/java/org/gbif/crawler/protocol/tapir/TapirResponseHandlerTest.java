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
package org.gbif.crawler.protocol.tapir;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class TapirResponseHandlerTest {

  @Test
  public void testConstructor() {
    TapirResponseHandler handler = new TapirResponseHandler();
    assertFalse(handler.isValidState());
    assertThrows(IllegalStateException.class, handler::isEndOfRecords);
    assertThrows(IllegalStateException.class, handler::getContentHash);
    assertThrows(IllegalStateException.class, handler::getRecordCount);
  }
}
