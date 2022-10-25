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
package org.gbif.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableSet;

/** A simple file reader for CSV files that are the result of Hue queries. */
public class HueCsvReader {

  private static final Logger LOG = LoggerFactory.getLogger(HueCsvReader.class);
  private static final Set<String> HEADER_FIELDS =
      ImmutableSet.of("id", "dataset_id", "key", "dataset_key");

  /**
   * Read the CSV file export from a Hue query for keys (e.g. select key from occurrence where xxx
   * or select dataset_key from occurrence where xxx). Properly handles the header row "id" and
   * double quotes that Hue adds.
   *
   * @param keyFileName the file to read
   * @return the keys as Strings
   */
  public static List<String> readKeys(String keyFileName) {
    List<String> keys = Lists.newArrayList();
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(keyFileName));
      String line;
      while ((line = br.readLine()) != null) {
        String key = line.replaceAll("\"", "");
        if (!HEADER_FIELDS.contains(key)) {
          keys.add(key);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.error("Could not find csv key file [{}]. Exiting", keyFileName, e);
    } catch (IOException e) {
      LOG.error("Error while reading csv key file [{}]. Exiting", keyFileName, e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.warn("Couldn't close key file. Attempting to continue anyway.", e);
        }
      }
    }

    return keys;
  }
}
