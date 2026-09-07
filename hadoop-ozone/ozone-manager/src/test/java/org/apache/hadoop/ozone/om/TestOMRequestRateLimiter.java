/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMRateLimitExceededException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOMRequestRateLimiter {

  private static Stream<Arguments> operations() {
    return Stream.of(
        Arguments.of("listKeys",
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_KEY,
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_DEFAULT,
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_TIMEOUT_KEY,
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_TIMEOUT_DEFAULT),
        Arguments.of("listStatus",
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_KEY,
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_DEFAULT,
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_TIMEOUT_KEY,
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_TIMEOUT_DEFAULT));
  }

  @ParameterizedTest
  @MethodSource("operations")
  void disabledLimiterAllowsRepeatedRequests(String operation,
      String rateLimitKey, int defaultRateLimit,
      String timeoutKey, int defaultTimeout) {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setInt(rateLimitKey, 0);

    OMRequestRateLimiter limiter = OMRequestRateLimiter.fromConfiguration(
        configuration, operation, rateLimitKey, defaultRateLimit,
        timeoutKey, defaultTimeout);

    assertDoesNotThrow(limiter::acquire);
    assertDoesNotThrow(limiter::acquire);
  }

  @ParameterizedTest
  @MethodSource("operations")
  void zeroTimeoutRejectsImmediatelyWhenRateIsExceeded(String operation,
      String rateLimitKey, int defaultRateLimit,
      String timeoutKey, int defaultTimeout) {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setInt(rateLimitKey, 1);
    configuration.setInt(timeoutKey, 0);

    OMRequestRateLimiter limiter = OMRequestRateLimiter.fromConfiguration(
        configuration, operation, rateLimitKey, defaultRateLimit,
        timeoutKey, defaultTimeout);

    assertDoesNotThrow(limiter::acquire);
    assertThrows(OMRateLimitExceededException.class, limiter::acquire);
  }

  @ParameterizedTest
  @MethodSource("operations")
  void negativeTimeoutIsRejected(String operation,
      String rateLimitKey, int defaultRateLimit,
      String timeoutKey, int defaultTimeout) {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setInt(rateLimitKey, 1);
    configuration.setInt(timeoutKey, -1);

    assertThrows(IllegalArgumentException.class,
        () -> OMRequestRateLimiter.fromConfiguration(
            configuration, operation, rateLimitKey, defaultRateLimit,
            timeoutKey, defaultTimeout));
  }
}
