package org.gbif.crawler;

import java.util.UUID;

import com.google.common.base.Charsets;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

/**
 * Needed by the Curator Queue to serialize and deserialize UUIDs.
 */
public class UuidSerializer implements QueueSerializer<UUID> {

  @Override
  public byte[] serialize(UUID item) {
    return item.toString().getBytes(Charsets.UTF_8);
  }

  @Override
  public UUID deserialize(byte[] bytes) {
    return UUID.fromString(new String(bytes, Charsets.UTF_8));
  }

}
