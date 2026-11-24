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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;

/**
 * Resolves host names and refuses any that resolve to a non-public address (loopback, private,
 * link-local, cloud metadata, etc.), so publisher-supplied URLs can't reach internal services
 * (SSRF). Used for every connection, so redirects and DNS rebinding are covered as well.
 */
public class PublicAddressDnsResolver implements DnsResolver {

  private final DnsResolver delegate;
  private final Set<String> allowedHosts;

  /** @param allowedHosts host names that may resolve to non-public addresses */
  public PublicAddressDnsResolver(Collection<String> allowedHosts) {
    this(SystemDefaultDnsResolver.INSTANCE, allowedHosts);
  }

  PublicAddressDnsResolver(DnsResolver delegate, Collection<String> allowedHosts) {
    this.delegate = delegate;
    this.allowedHosts =
        allowedHosts.stream().map(h -> h.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
  }

  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    InetAddress[] addresses = delegate.resolve(host);
    if (allowedHosts.contains(host.toLowerCase(Locale.ROOT))) {
      return addresses;
    }
    for (InetAddress address : addresses) {
      if (!isPublic(address)) {
        throw new BlockedAddressException(host, address);
      }
    }
    return addresses;
  }

  static boolean isPublic(InetAddress address) {
    if (address.isAnyLocalAddress()
        || address.isLoopbackAddress()
        || address.isLinkLocalAddress()
        || address.isSiteLocalAddress()
        || address.isMulticastAddress()) {
      return false;
    }
    byte[] b = address.getAddress();
    if (address instanceof Inet4Address) {
      int first = b[0] & 0xff;
      int second = b[1] & 0xff;
      return first != 0 // "this" network
          && !(first == 100 && second >= 64 && second <= 127) // carrier-grade NAT
          && !(first == 192 && second == 0 && (b[2] & 0xff) == 0) // IETF protocol assignments
          && first < 240; // reserved and broadcast
    }
    if (address instanceof Inet6Address) {
      if ((b[0] & 0xfe) == 0xfc) {
        return false; // unique local fc00::/7
      }
      if (isIpv4Embedded(b)) {
        try {
          byte[] v4 = {b[12], b[13], b[14], b[15]};
          return isPublic(InetAddress.getByAddress(v4));
        } catch (UnknownHostException e) {
          return false;
        }
      }
    }
    return true;
  }

  /** IPv4-compatible ::a.b.c.d and NAT64 64:ff9b::a.b.c.d addresses. */
  private static boolean isIpv4Embedded(byte[] b) {
    boolean compatible = true;
    for (int i = 0; i < 12; i++) {
      compatible &= b[i] == 0;
    }
    boolean nat64 = (b[0] & 0xff) == 0x00 && (b[1] & 0xff) == 0x64 && (b[2] & 0xff) == 0xff
        && (b[3] & 0xff) == 0x9b;
    for (int i = 4; nat64 && i < 12; i++) {
      nat64 = b[i] == 0;
    }
    return compatible || nat64;
  }

  /** Thrown when a host resolves to an address the crawler must not connect to. */
  public static class BlockedAddressException extends UnknownHostException {

    private static final long serialVersionUID = 1L;

    BlockedAddressException(String host, InetAddress address) {
      super(host + " resolves to non-public address " + address.getHostAddress());
    }
  }
}
