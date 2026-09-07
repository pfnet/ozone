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

package org.apache.hadoop.ozone.om.exceptions;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.ipc_.RemoteException;
import org.junit.jupiter.api.Test;

/**
 * Tests recognition of an OM rate limit refusal in the shapes it actually
 * reaches a client in.
 */
class TestOMRateLimitExceededException {

  private static final String MESSAGE = "Rate limit exceeded for listKeys";

  @Test
  void recognisesDirectInstance() {
    assertTrue(OMRateLimitExceededException.isRateLimitExceeded(
        new OMRateLimitExceededException(MESSAGE)));
  }

  /**
   * The shape the S3 Gateway sees: OzoneBucket.KeyIterator#hasNext rethrows the
   * IOException wrapped in a RuntimeException.
   */
  @Test
  void recognisesItWrappedInRuntimeException() {
    assertTrue(OMRateLimitExceededException.isRateLimitExceeded(
        new RuntimeException(new OMRateLimitExceededException(MESSAGE))));
  }

  /**
   * The shape before unwrapRemoteException() has reconstructed the concrete
   * type -- only the class name is carried.
   */
  @Test
  void recognisesRemoteExceptionByClassName() {
    RemoteException remote = new RemoteException(
        OMRateLimitExceededException.class.getName(), MESSAGE);
    assertTrue(OMRateLimitExceededException.isRateLimitExceeded(remote));
    assertTrue(OMRateLimitExceededException.isRateLimitExceeded(
        new RuntimeException(remote)));
  }

  @Test
  void ignoresOtherRemoteExceptions() {
    assertFalse(OMRateLimitExceededException.isRateLimitExceeded(
        new RemoteException(OMNotLeaderException.class.getName(),
            "not the leader")));
  }

  @Test
  void ignoresUnrelatedExceptions() {
    assertFalse(OMRateLimitExceededException.isRateLimitExceeded(
        new IOException("disk on fire")));
    assertFalse(OMRateLimitExceededException.isRateLimitExceeded(null));
  }

  /** A cyclic cause chain must not spin forever. */
  @Test
  void terminatesOnCyclicCauseChain() {
    IOException a = new IOException("a");
    IOException b = new IOException("b");
    a.initCause(b);
    b.initCause(a);
    assertFalse(OMRateLimitExceededException.isRateLimitExceeded(a));
  }
}
