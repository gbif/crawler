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

import org.gbif.crawler.client.PublicAddressDnsResolver;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;

import com.beust.jcommander.Parameter;

import lombok.ToString;

/**
 * Which hosts the crawler may connect to when fetching publisher URLs. By default only public
 * addresses are allowed, so endpoints can't be pointed at internal services (SSRF).
 */
@ToString
public class HostRestrictions {

  @Parameter(
      names = "--allow-private-addresses",
      description = "Allow publisher URLs that resolve to private, loopback or link-local addresses")
  public boolean allowPrivateAddresses = false;

  @Parameter(
      names = "--allowed-hosts",
      description = "Host names that may resolve to private addresses, e.g. a hosted IPT")
  public List<String> allowedHosts = new ArrayList<>();

  public DnsResolver dnsResolver() {
    return allowPrivateAddresses
        ? SystemDefaultDnsResolver.INSTANCE
        : new PublicAddressDnsResolver(allowedHosts);
  }
}
