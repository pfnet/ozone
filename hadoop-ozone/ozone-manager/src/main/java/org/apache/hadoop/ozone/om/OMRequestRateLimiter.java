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

import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.exceptions.OMRateLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rate limiter for expensive read-only OM requests. */
final class OMRequestRateLimiter {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMRequestRateLimiter.class);

  private final String operation;
  private final RateLimiter rateLimiter;
  private final Duration timeout;

  private OMRequestRateLimiter(String operation, RateLimiter rateLimiter,
      Duration timeout) {
    this.operation = operation;
    this.rateLimiter = rateLimiter;
    this.timeout = timeout;
  }

  static OMRequestRateLimiter fromConfiguration(
      ConfigurationSource configuration, String operation,
      String rateLimitKey, int defaultRateLimit,
      String timeoutKey, int defaultTimeout) {
    int permitsPerSecond = configuration.getInt(
        rateLimitKey, defaultRateLimit);
    if (permitsPerSecond <= 0) {
      LOG.info("{} rate limit disabled: permitsPerSecond={}",
          operation, permitsPerSecond);
      return new OMRequestRateLimiter(operation, null, Duration.ZERO);
    }

    int timeoutSeconds = configuration.getInt(timeoutKey, defaultTimeout);
    if (timeoutSeconds < 0) {
      throw new IllegalArgumentException(
          timeoutKey + " must be zero or greater");
    }

    Duration timeout = Duration.ofSeconds(timeoutSeconds);
    LOG.info("{} rate limit enabled: permitsPerSecond={}, timeout={}",
        operation, permitsPerSecond, timeout);
    return new OMRequestRateLimiter(
        operation, RateLimiter.create(permitsPerSecond), timeout);
  }

  void acquire() throws OMRateLimitExceededException {
    if (rateLimiter != null && !rateLimiter.tryAcquire(timeout)) {
      throw new OMRateLimitExceededException(
          "Rate limit exceeded for " + operation);
    }
  }
}
