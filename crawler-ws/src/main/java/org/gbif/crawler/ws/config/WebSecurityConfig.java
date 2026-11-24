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
package org.gbif.crawler.ws.config;

import java.util.Arrays;
import java.util.Collections;

import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementPortType;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.info.InfoEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

@Configuration
@EnableWebSecurity
public class WebSecurityConfig {

  @Bean
  public SecurityFilterChain filterChain(HttpSecurity http, Environment environment)
      throws Exception {
    // On a separate management port (not routed by the ingress) Spring Boot Admin gets every
    // actuator endpoint; on the public port only health and info, as others can change or leak
    // state.
    boolean separateManagementPort =
      ManagementPortType.get(environment) == ManagementPortType.DIFFERENT;

    http.httpBasic(AbstractHttpConfigurer::disable)
      .csrf(AbstractHttpConfigurer::disable)
      .cors(cors -> cors.configurationSource(corsConfigurationSource()))
      .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
      .authorizeHttpRequests(
        auth ->
          auth
            .requestMatchers(EndpointRequest.to(HealthEndpoint.class, InfoEndpoint.class))
            .permitAll()
            .requestMatchers(EndpointRequest.toAnyEndpoint())
            .access(
              (authentication, context) -> new AuthorizationDecision(separateManagementPort))
            .anyRequest()
            .permitAll());

    return http.build();
  }

  @Bean
  CorsConfigurationSource corsConfigurationSource() {
    // CorsFilter only applies this if the origin header is present in the request
    CorsConfiguration configuration = new CorsConfiguration();
    configuration.setAllowedHeaders(Arrays.asList("authorization", "content-type"));
    configuration.setAllowedOrigins(Collections.singletonList("*"));
    // The API is read-only
    configuration.setAllowedMethods(Arrays.asList("HEAD", "GET", "OPTIONS"));
    configuration.setExposedHeaders(
      Arrays.asList(
        "Access-Control-Allow-Origin",
        "Access-Control-Allow-Methods",
        "Access-Control-Allow-Headers"));
    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    // API only: actuator responses must not be readable cross-origin
    source.registerCorsConfiguration("/dataset/**", configuration);
    return source;
  }
}
