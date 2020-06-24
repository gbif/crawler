/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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
package org.gbif.crawler.exception;

/**
 * Indicates an issue with the protocol handling, such as a response in an unexpected format.
 *
 * <p>The use of {@link java.net.ProtocolException} was discarded, to differentiate that this deals
 * with the harvesting protocol such as DiGIR, BioCASe or TAPIR, and not underlying transport layer
 * protocols such as TCP, to which the {@link java.net.ProtocolException} is typically associated.
 */
public class ProtocolException extends Exception {

  private static final long serialVersionUID = -4218627840124526625L;

  public ProtocolException(String message) {
    super(message);
  }

  public ProtocolException(Throwable cause) {
    super(cause);
  }

  public ProtocolException(String message, Throwable cause) {
    super(message, cause);
  }
}
