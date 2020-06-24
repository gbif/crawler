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
package org.gbif.crawler.protocol;

import org.gbif.crawler.ResponseHandler;
import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.exception.TransportException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.apache.http.HttpResponse;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLOutputFactory2;
import org.codehaus.stax2.XMLStreamReader2;
import org.codehaus.stax2.XMLStreamWriter2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A base class for response handlers.
 *
 * <p>This class takes care of converting a {@link HttpResponse} into a byte[] which is then
 * converted to a List of bytes. You can get the underlying array back by using Guava: {@link
 * Bytes#toArray(java.util.Collection)}}.
 *
 * <p>A hash code is automatically calculated for the content of the response. The various getters
 * throw an {@link IllegalStateException} if the last response was not valid so check {@link
 * #isValidState} before calling any of those. An alternative indicator are the exceptions thrown by
 * {@link #handleResponse(HttpResponse)}. If any is thrown during processing the Response handler is
 * in an illegal state until a successful response has been handled.
 *
 * <p>This class is not thread-safe.
 */
@NotThreadSafe
public abstract class AbstractResponseHandler implements ResponseHandler<HttpResponse, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractResponseHandler.class);

  private static final XMLInputFactory2 XIF = (XMLInputFactory2) XMLInputFactory.newInstance();
  private static final XMLOutputFactory2 XOF = (XMLOutputFactory2) XMLOutputFactory.newInstance();

  private Optional<Integer> recordCount = Optional.absent();
  private Optional<Boolean> endOfRecords = Optional.absent();
  private Optional<Long> contentHash;

  private boolean validState = false;

  /**
   * This needs to be implemented by subclasses to do actual processing on the XML data. This method
   * is called for every single stream event (which includes whitespace etc.) so implementations
   * need to check the event type and make sure to work on the correct elements.
   *
   * @param reader to use
   * @throws XMLStreamException if there was an error with the XML parsing (e.g. invalid XML)
   * @throws ProtocolException if there is a problem with the (well-formed) XML response (e.g.
   *     missing attribute)
   */
  protected abstract void process(XMLStreamReader2 reader)
      throws XMLStreamException, FatalCrawlException, ProtocolException;

  /**
   * Needs to be implemented by subclasses to indicate to the parent class what the XML element is
   * which delimits the actual content of a result. Everything inside this element including the
   * element itself is hashed.
   *
   * @return with the content element name
   */
  protected abstract boolean shouldHash(XMLStreamReader2 reader);

  @Override
  public boolean isValidState() {
    return validState;
  }

  private void initializeInternal() {
    recordCount = Optional.absent();
    endOfRecords = Optional.absent();
    contentHash = Optional.absent();
    validState = false;
  }

  /** To be implemented to initialize internal state before every new response is being handled. */
  protected abstract void initialize();

  /**
   * This is an implementation that processes a {@link HttpResponse} and produces a List of Bytes.
   *
   * @param response to be handled
   * @return the byte array corresponding to the response we received. Wrapped in a List of Bytes
   *     because this is extending a typed interface. This list is guaranteed to be convertable to a
   *     primitive {@code byte[]} using Guava's {@link Bytes#toArray(java.util.Collection)}}.
   * @throws ProtocolException if there are any problems with the content received
   * @throws TransportException if there are any problems receiving the response
   */
  @Override
  public final List<Byte> handleResponse(HttpResponse response)
      throws FatalCrawlException, ProtocolException, TransportException {
    checkNotNull(response);

    initializeInternal();

    initialize();

    // First of all we convert the response into a byte array
    byte[] bytes;
    try {
      bytes = ByteStreams.toByteArray(response.getEntity().getContent());
    } catch (IOException e) {
      throw new TransportException(e);
    }

    Hasher hasher = Hashing.md5().newHasher();

    // The reader is used to parse the XML response and the writer is used to hash parts of the
    // document
    XMLStreamReader2 reader = null;
    XMLStreamWriter2 writer = null;
    boolean didHash = false;
    try {
      reader = (XMLStreamReader2) XIF.createXMLStreamReader(new ByteArrayInputStream(bytes));
      writer = (XMLStreamWriter2) XOF.createXMLStreamWriter(Funnels.asOutputStream(hasher));
      writer.writeStartElement("wrapper");

      while (reader.next() != XMLStreamConstants.END_DOCUMENT) {
        // Hand off processing to subclasses
        process(reader);

        if (shouldHash(reader)) {
          didHash = true;
          writer.copyEventFromReader(reader, true);
        }
      }

      writer.writeEndElement();

    } catch (XMLStreamException e) {
      throw new ProtocolException(e);
    } finally {
      closeQuietly(reader);
      closeQuietly(writer);
    }

    if (didHash) {
      contentHash = Optional.of(hasher.hash().asLong());
    }

    validState = true;

    return Bytes.asList(bytes);
  }

  @Override
  public final Optional<Integer> getRecordCount() {
    checkState(validState);

    return recordCount;
  }

  protected final void setRecordCount(Optional<Integer> recordCount) {
    this.recordCount = checkNotNull(recordCount);
  }

  protected final void setEndOfRecords(Optional<Boolean> endOfRecords) {
    this.endOfRecords = checkNotNull(endOfRecords);
  }

  @Override
  public final Optional<Boolean> isEndOfRecords() {
    checkState(validState, "Last response was not valid");

    return endOfRecords;
  }

  @Override
  public final Optional<Long> getContentHash() {
    checkState(validState, "Last response was not valid");

    return contentHash;
  }

  /**
   * Closes a XMLStreamReader quietly. Exceptions are being swallowed and logged.
   *
   * @param reader to close
   */
  private static void closeQuietly(@Nullable XMLStreamReader reader) {
    if (reader == null) {
      return;
    }
    try {
      reader.close();
    } catch (XMLStreamException e) {
      LOG.warn("Error closing XML reader", e);
    }
  }

  /**
   * Closes a XMLStreamWriter quietly. Exceptions are being swallowed and logged.
   *
   * @param writer to close
   */
  private static void closeQuietly(@Nullable XMLStreamWriter writer) {
    if (writer == null) {
      return;
    }
    try {
      writer.close();
    } catch (XMLStreamException e) {
      LOG.debug("Error closing XML writer", e);
    }
  }

  protected static boolean isElement(XMLStreamReader reader, String name, boolean start) {
    return reader.getEventType()
            == (start ? XMLStreamConstants.START_ELEMENT : XMLStreamConstants.END_ELEMENT)
        && reader.getLocalName().equals(name);
  }
}
