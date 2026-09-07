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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ipc_.RetriableException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Rate limits expensive read-only OM requests, keyed by request type.
 *
 * <p>This is acquired from
 * {@code OzoneManagerProtocolServerSideTranslatorPB#submitReadRequestToOM}
 * before the request is dispatched, rather than from inside the
 * {@link OzoneManager} read methods, because of where the exception ends up.
 * {@code OzoneManagerRequestHandler#handleReadRequest} wraps its whole dispatch
 * switch in {@code catch (IOException)} and maps anything that is not an
 * {@code OMException} to {@code Status.INTERNAL_ERROR} in the response body.
 * {@link RetriableException} extends {@code IOException}, so a permit failure
 * raised below that point loses its type and reaches the client as a plain
 * INTERNAL_ERROR that no retry policy can act on. Raised above the dispatcher
 * it propagates as a {@code ServiceException}, keeps its class name on the
 * wire, and the client can tell a throttled request from a real failure.
 *
 * <p>Acquiring before dispatch is also cheaper: the request is rejected before
 * leader/lease evaluation, ACL checks and snapshot resolution, so a throttled
 * request costs almost nothing to refuse.
 *
 * <p>The {@code Light} variants must be listed explicitly. They happen to
 * delegate to their non-Light counterparts inside {@link OzoneManager} today,
 * which is what let a limiter on the two non-Light methods cover all four
 * request types, but that delegation is an upstream implementation detail --
 * and one upstream has reason to remove, since {@code listKeysLight} currently
 * builds full {@code OmKeyInfo} objects only to downgrade them. The Light
 * variants are what S3G and ofs actually use, so relying on it is a trap.
 */
public final class OMReadRequestThrottler {

  private final Map<Type, OMRequestRateLimiter> limiters;

  private OMReadRequestThrottler(Map<Type, OMRequestRateLimiter> limiters) {
    this.limiters = limiters;
  }

  public static OMReadRequestThrottler fromConfiguration(
      ConfigurationSource configuration) {
    OMRequestRateLimiter listKeysRateLimiter =
        OMRequestRateLimiter.fromConfiguration(
            configuration, "listKeys",
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_KEY,
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_DEFAULT,
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_TIMEOUT_KEY,
            OMConfigKeys.OZONE_OM_LISTKEYS_RATELIMIT_TIMEOUT_DEFAULT);
    OMRequestRateLimiter listStatusRateLimiter =
        OMRequestRateLimiter.fromConfiguration(
            configuration, "listStatus",
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_KEY,
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_DEFAULT,
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_TIMEOUT_KEY,
            OMConfigKeys.OZONE_OM_LISTSTATUS_RATELIMIT_TIMEOUT_DEFAULT);

    Map<Type, OMRequestRateLimiter> map = new EnumMap<>(Type.class);
    map.put(Type.ListKeys, listKeysRateLimiter);
    map.put(Type.ListKeysLight, listKeysRateLimiter);
    map.put(Type.ListStatus, listStatusRateLimiter);
    map.put(Type.ListStatusLight, listStatusRateLimiter);

    return new OMReadRequestThrottler(Collections.unmodifiableMap(map));
  }

  /**
   * Acquires a permit for the given request type. Request types that are not
   * rate limited pass through untouched.
   *
   * @throws RetriableException if no permit is available within the configured
   *     timeout. The caller is expected to let this reach the RPC layer.
   */
  public void acquire(Type cmdType) throws RetriableException {
    OMRequestRateLimiter limiter = limiters.get(cmdType);
    if (limiter != null) {
      limiter.acquire();
    }
  }

  @VisibleForTesting
  Set<Type> getThrottledTypes() {
    return limiters.keySet();
  }
}
