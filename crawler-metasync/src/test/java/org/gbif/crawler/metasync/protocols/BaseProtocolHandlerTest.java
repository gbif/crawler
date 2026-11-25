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
package org.gbif.crawler.metasync.protocols;

import org.gbif.crawler.metasync.protocols.digir.DigirMetadataSynchronizer;
import org.gbif.crawler.metasync.protocols.digir.model.DigirMetadata;

import org.apache.commons.digester3.Digester;
import org.apache.http.client.HttpClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class BaseProtocolHandlerTest {

  @Test
  public void testDigesterIsNamespaceAware() {
    Digester digester =
        new DigirMetadataSynchronizer(mock(HttpClient.class)).newDigester(DigirMetadata.class);

    assertTrue(digester.getNamespaceAware());
  }
}
