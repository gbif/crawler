package org.gbif.crawler;

import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.vocabulary.EndpointType;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

/**
 * Compares two Endpoints.
 * <p></p>
 * It does so by using a priority list of Endpoint Types.
 * This Comparator will throw a {@link ClassCastException} exception if a non supported EndpointType is passed in. It
 * also does not support {@code null} values.
 * <p></p>
 * The priority list is as follows (most to least important):
 * <ol>
 * <li>DwC-A</li>
 * <li>TAPIR</li>
 * <li>BioCASe</li>
 * <li>DiGIR</li>
 * <li>DiGIR (Manis)</li>
 * <li>EML</li>
 * </ol>
 */
public class EndpointPriorityComparator implements Comparator<Endpoint>, Serializable {

  /* Note: Due to legacy reasons this is a Comparator itself instead of just using the PRIORITY_COMPARATOR.
   It should be easy to remove this class or change it into a factory */

  // Priorities from lowest to highest
  public static final List<EndpointType> PRIORITIES = ImmutableList.of(
      EndpointType.EML,
      EndpointType.DIGIR_MANIS,
      EndpointType.DIGIR,
      EndpointType.BIOCASE,
      EndpointType.TAPIR,
      EndpointType.BIOCASE_XML_ARCHIVE,
      EndpointType.DWC_ARCHIVE
  );

  private static final long serialVersionUID = 8085216142750609841L;
  private static final Ordering<EndpointType> PRIORITY_COMPARATOR = Ordering.explicit(PRIORITIES);

  @Override
  public int compare(Endpoint endpoint1, Endpoint endpoint2) {
    return PRIORITY_COMPARATOR.compare(endpoint1.getType(), endpoint2.getType());
  }

}
