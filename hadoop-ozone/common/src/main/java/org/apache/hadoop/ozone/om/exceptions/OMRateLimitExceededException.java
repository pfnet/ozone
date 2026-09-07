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

import java.io.IOException;
import org.apache.hadoop.ipc_.RemoteException;

/**
 * Thrown by the OM when an expensive read request is refused because its
 * configured rate limit is exhausted.
 *
 * <p>This lives in ozone-common rather than ozone-manager so that clients --
 * in particular the Ozone client embedded in the S3 Gateway -- can identify it
 * after {@code RemoteException.unwrapRemoteException()} reconstructs it by
 * class name. That is what {@link #isRateLimitExceeded} relies on, and it is
 * why the single-String constructor must be kept.
 *
 * <p>Deliberately <em>not</em> a {@code RetriableException}. Retrying inside
 * the caller is the wrong response for a gateway: the S3 Gateway serves each
 * request on a Jetty worker thread, so sleeping there holds the thread, starves
 * unrelated requests, and keeps offering the OM the same request rate. The
 * correct behaviour is to refuse immediately and let the S3 client back off --
 * see the {@code SLOW_DOWN} mapping in {@code S3ErrorTable}, which botocore
 * treats as throttling and retries with exponential backoff.
 */
public class OMRateLimitExceededException extends IOException {

  /** Guards against cyclic cause chains. */
  private static final int MAX_CAUSE_DEPTH = 32;

  public OMRateLimitExceededException(String message) {
    super(message);
  }

  /**
   * Returns true if {@code throwable} is, or was caused by, a rate limit
   * refusal from the OM.
   *
   * <p>The exception reaches a client in one of two shapes depending on how far
   * it has been unwrapped: as this type, or still as a {@link RemoteException}
   * naming it. Both are recognised. The cause chain is walked because the Ozone
   * client wraps it on the way out -- {@code OzoneBucket.KeyIterator#hasNext}
   * rethrows it inside a {@link RuntimeException}.
   */
  public static boolean isRateLimitExceeded(Throwable throwable) {
    Throwable t = throwable;
    // Bounded rather than walked to the end: Throwable only rejects direct
    // self-causation, so a longer cause cycle is constructible and would spin.
    for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH;
        depth++, t = t.getCause()) {
      if (t instanceof OMRateLimitExceededException) {
        return true;
      }
      if (t instanceof RemoteException && OMRateLimitExceededException.class
          .getName().equals(((RemoteException) t).getClassName())) {
        return true;
      }
    }
    return false;
  }
}
