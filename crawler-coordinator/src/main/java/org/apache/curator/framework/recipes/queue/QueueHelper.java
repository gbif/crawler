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
package org.apache.curator.framework.recipes.queue;

/**
 * This can be used to serialize and deserialize data we published as part of a Queued item using
 * Curator.
 *
 * <p>It has to live in this package because {@link ItemSerializer} is package-private. This class
 * is just a thin wrapper to expose those methods.
 */
public class QueueHelper {

  public static <T> MultiItem<T> deserialize(byte[] bytes, QueueSerializer<T> serializer)
      throws Exception {
    return ItemSerializer.deserialize(bytes, serializer);
  }

  public static <T> T deserializeSingle(byte[] bytes, QueueSerializer<T> serializer)
      throws Exception {
    return ItemSerializer.deserialize(bytes, serializer).nextItem();
  }

  public static <T> byte[] serialize(MultiItem<T> items, QueueSerializer<T> serializer)
      throws Exception {
    return ItemSerializer.serialize(items, serializer);
  }

  public static <T> byte[] serialize(T item, QueueSerializer<T> serializer) throws Exception {
    return ItemSerializer.serialize(new SimpleMultiItem<T>(item), serializer);
  }

  private static class SimpleMultiItem<T> implements MultiItem<T> {

    private T item;

    private SimpleMultiItem(T item) {
      this.item = item;
    }

    @Override
    public T nextItem() throws Exception {
      T returnItem = item;
      item = null;
      return returnItem;
    }
  }

  private QueueHelper() {
    throw new UnsupportedOperationException("Can't initialize class");
  }
}
