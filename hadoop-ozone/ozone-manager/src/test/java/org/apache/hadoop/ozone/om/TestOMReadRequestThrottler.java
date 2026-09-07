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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc_.RetriableException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMReadRequestThrottler}, in particular that every request
 * type intended to be rate limited actually is.
 */
class TestOMReadRequestThrottler {

  /**
   * List request types that are deliberately not rate limited. These either
   * return a bounded result or are administrative, unlike the key and status
   * listings that iterate a bucket a page at a time.
   *
   * <p>Kept explicit so that {@link #everyListRequestTypeIsClassified()} fails
   * when a new List request type appears upstream, forcing a decision instead
   * of a silent pass-through.
   */
  private static final Set<Type> DELIBERATELY_UNTHROTTLED = EnumSet.of(
      Type.ListVolume,
      Type.ListBuckets,
      Type.ListMultiPartUploadParts,
      Type.ListMultipartUploads,
      Type.ListTrash,
      Type.ListTenant,
      Type.ListSnapshot,
      Type.ListSnapshotDiffJobs,
      Type.ListOpenFiles);

  private static OMReadRequestThrottler throttlerWithSinglePermit() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setInt(OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_KEY, 1);
    configuration.setInt(
        OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_TIMEOUT_KEY, 0);
    configuration.setInt(OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_KEY, 1);
    configuration.setInt(
        OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_TIMEOUT_KEY, 0);
    return OMReadRequestThrottler.fromConfiguration(configuration);
  }

  @Test
  void listKeysAndListKeysLightShareOneBudget() {
    OMReadRequestThrottler throttler = throttlerWithSinglePermit();

    assertDoesNotThrow(() -> throttler.acquire(Type.ListKeys));
    assertThrows(RetriableException.class,
        () -> throttler.acquire(Type.ListKeysLight));
  }

  @Test
  void listStatusAndListStatusLightShareOneBudget() {
    OMReadRequestThrottler throttler = throttlerWithSinglePermit();

    assertDoesNotThrow(() -> throttler.acquire(Type.ListStatus));
    assertThrows(RetriableException.class,
        () -> throttler.acquire(Type.ListStatusLight));
  }

  @Test
  void listKeysAndListStatusBudgetsAreIndependent() {
    OMReadRequestThrottler throttler = throttlerWithSinglePermit();

    assertDoesNotThrow(() -> throttler.acquire(Type.ListKeys));
    assertThrows(RetriableException.class,
        () -> throttler.acquire(Type.ListKeys));

    assertDoesNotThrow(() -> throttler.acquire(Type.ListStatus));
  }

  @Test
  void unthrottledTypesPassThrough() {
    OMReadRequestThrottler throttler = throttlerWithSinglePermit();

    assertDoesNotThrow(() -> throttler.acquire(Type.ListKeys));
    assertThrows(RetriableException.class,
        () -> throttler.acquire(Type.ListKeys));

    // Exhausting the listKeys budget must not affect anything else.
    assertDoesNotThrow(() -> throttler.acquire(Type.LookupKey));
    for (Type type : DELIBERATELY_UNTHROTTLED) {
      assertDoesNotThrow(() -> throttler.acquire(type));
    }
  }

  @Test
  void disabledLimiterAllowsRepeatedRequests() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setInt(OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_KEY, 0);
    configuration.setInt(OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_KEY, 0);
    OMReadRequestThrottler throttler =
        OMReadRequestThrottler.fromConfiguration(configuration);

    assertDoesNotThrow(() -> throttler.acquire(Type.ListKeys));
    assertDoesNotThrow(() -> throttler.acquire(Type.ListKeysLight));
    assertDoesNotThrow(() -> throttler.acquire(Type.ListStatus));
    assertDoesNotThrow(() -> throttler.acquire(Type.ListStatusLight));
  }

  /**
   * Every List request type must be either rate limited or explicitly listed
   * as deliberately unthrottled. Without this, a new List type added upstream
   * would silently bypass the limiter -- which is exactly how the Light
   * variants could have been missed when the limiter lived on the two
   * non-Light OzoneManager methods and relied on internal delegation.
   */
  @Test
  void everyListRequestTypeIsClassified() {
    OMReadRequestThrottler throttler = throttlerWithSinglePermit();
    Set<Type> throttled = throttler.getThrottledTypes();

    Set<Type> unclassified = Stream.of(Type.values())
        .filter(type -> type.name().startsWith("List"))
        .filter(type -> !throttled.contains(type))
        .filter(type -> !DELIBERATELY_UNTHROTTLED.contains(type))
        .collect(Collectors.toCollection(() -> EnumSet.noneOf(Type.class)));

    assertEquals(EnumSet.noneOf(Type.class), unclassified,
        "New List request type(s) found. Add each one to the throttler in "
            + "OMReadRequestThrottler.fromConfiguration, or to "
            + "DELIBERATELY_UNTHROTTLED in this test.");
  }

  @Test
  void throttledTypesAreTheFourListingRequests() {
    OMReadRequestThrottler throttler = throttlerWithSinglePermit();

    assertEquals(
        EnumSet.of(Type.ListKeys, Type.ListKeysLight,
            Type.ListStatus, Type.ListStatusLight),
        EnumSet.copyOf(throttler.getThrottledTypes()));
  }
}
