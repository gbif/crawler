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
package org.gbif.crawler.dwca.metasync;

import org.gbif.crawler.common.ZooKeeperConfiguration;
import org.gbif.crawler.dwca.DwcaConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.ParametersDelegate;

import lombok.ToString;

@ToString(callSuper = true)
public class DwcaMetasyncConfiguration extends DwcaConfiguration {

  @ParametersDelegate @Valid @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

}
