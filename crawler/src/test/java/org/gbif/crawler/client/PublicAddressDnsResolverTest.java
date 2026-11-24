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
package org.gbif.crawler.client;

import org.gbif.crawler.client.PublicAddressDnsResolver.BlockedAddressException;

import java.net.InetAddress;
import java.util.List;

import org.apache.http.conn.DnsResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublicAddressDnsResolverTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0.0.0.0",
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "fe80::1",
        "fd00::1",
        "::ffff:127.0.0.1",
        "::ffff:169.254.169.254",
        "::127.0.0.1",
        "64:ff9b::a9fe:a9fe"
      })
  public void testNonPublicAddresses(String address) throws Exception {
    assertFalse(PublicAddressDnsResolver.isPublic(InetAddress.getByName(address)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"8.8.8.8", "130.225.43.2", "2001:4860:4860::8888", "64:ff9b::808:808"})
  public void testPublicAddresses(String address) throws Exception {
    assertTrue(PublicAddressDnsResolver.isPublic(InetAddress.getByName(address)));
  }

  @Test
  public void testBlocksHostWithAnyPrivateAddress() {
    PublicAddressDnsResolver resolver =
        new PublicAddressDnsResolver(fixed("8.8.8.8", "10.0.0.1"), List.of());
    assertThrows(BlockedAddressException.class, () -> resolver.resolve("rebind.example.org"));
  }

  @Test
  public void testAllowedHost() throws Exception {
    PublicAddressDnsResolver resolver =
        new PublicAddressDnsResolver(fixed("10.0.0.1"), List.of("IPT.example.org"));
    assertEquals(1, resolver.resolve("ipt.example.org").length);
    assertThrows(BlockedAddressException.class, () -> resolver.resolve("other.example.org"));
  }

  private static DnsResolver fixed(String... addresses) {
    return host -> {
      InetAddress[] result = new InetAddress[addresses.length];
      for (int i = 0; i < addresses.length; i++) {
        result[i] = InetAddress.getByName(addresses[i]);
      }
      return result;
    };
  }
}
