package org.gbif.crawler;

import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.Country;
import org.gbif.ws.WebApplicationException;

import java.util.List;
import java.util.UUID;

import org.springframework.http.HttpStatus;

/**
 * Check restriction on handling sensitive data.
 */
public class RestrictionsHandler {

  private final List<String> denyCountries;

  private final OrganizationService organizationService;

  public RestrictionsHandler(List<String> denyCountries, OrganizationService organizationService) {
    this.denyCountries = denyCountries;
    this.organizationService = organizationService;
  }

  /**
   * Is the country in the list of denials.
   */
  public void checkCountryDenied(Country country) {
    if (country != null && denyCountries != null && (denyCountries.contains(country.getIso2LetterCode()) || denyCountries.contains(country.getIso3LetterCode()))) {
      throw new WebApplicationException("Illegal entity data", HttpStatus.FORBIDDEN);
    }
  }

  /**
   * Is the organization is the list of denials.
   */
  public void checkDenyPublisher(UUID organizationKey) {
    if (organizationKey != null) {
      Organization organization = organizationService.get(organizationKey);
      if (organization != null) {
        checkCountryDenied(organization.getCountry());
      }
    }
  }
}
