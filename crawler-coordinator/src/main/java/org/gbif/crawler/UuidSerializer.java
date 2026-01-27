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
package org.gbif.crawler;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.curator.framework.recipes.queue.QueueSerializer;

/** Needed by the Curator Queue to serialize and deserialize UUIDs. */
public class UuidSerializer implements QueueSerializer<UUID> {

  @Override
  public byte[] serialize(UUID item) {
    return item.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public UUID deserialize(byte[] bytes) {
    return UUID.fromString(new String(bytes, StandardCharsets.UTF_8));
  }
}
