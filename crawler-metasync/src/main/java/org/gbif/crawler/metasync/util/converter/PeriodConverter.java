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
package org.gbif.crawler.metasync.util.converter;

import java.time.Duration;
import java.time.format.DateTimeParseException;

import org.apache.commons.beanutils.Converter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Used by commons-digester (via commons-beanutils) to convert Strings into {@link Duration}.
 */
public class PeriodConverter implements Converter {

  @Override
  public Object convert(Class type, Object value) {
    checkNotNull(type, "type cannot be null");
    checkNotNull(value, "Value cannot be null");
    checkArgument(
      type.equals(Duration.class),
      "Conversion target should be java.time.Duration, but is %s",
      type);
    checkArgument(
      String.class.isAssignableFrom(value.getClass()),
      "Value should be a string, but is a %s",
      value.getClass());

    try {
      return Duration.parse((String) value);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("Could not parse duration: " + value, e);
    }
  }
}
