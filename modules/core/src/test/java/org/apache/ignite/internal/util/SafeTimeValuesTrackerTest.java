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

package org.apache.ignite.internal.util;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.junit.jupiter.api.Test;

/** Tests safe-time ordering diagnostics and the Ignite 3.1 waiter contract. */
class SafeTimeValuesTrackerTest {
    @Test
    void contextualUpdateCompletesExistingWaiters() {
        SafeTimeValuesTracker tracker = new SafeTimeValuesTracker(new HybridTimestamp(1, 0));
        HybridTimestamp timestamp = new HybridTimestamp(2, 0);
        CompletableFuture<Void> waiter = tracker.waitFor(timestamp);

        assertFalse(waiter.isDone());
        tracker.update(timestamp, 10, 3, "command");

        assertThat(waiter, willCompleteSuccessfully());
        assertEquals(timestamp, tracker.current());
        assertTrue(tracker.isEmpty());
    }

    @Test
    void legacyUpdateAlsoCompletesExistingWaiters() {
        SafeTimeValuesTracker tracker = new SafeTimeValuesTracker(new HybridTimestamp(1, 0));
        HybridTimestamp timestamp = new HybridTimestamp(2, 0);
        CompletableFuture<Void> waiter = tracker.waitFor(timestamp);

        tracker.update(timestamp, null);

        assertThat(waiter, willCompleteSuccessfully());
        assertEquals(timestamp, tracker.current());
    }

    @Test
    void reorderingReportsPreviousCommandAndDoesNotLowerSafeTime() {
        SafeTimeValuesTracker tracker = new SafeTimeValuesTracker(new HybridTimestamp(1, 0));
        HybridTimestamp current = new HybridTimestamp(3, 0);
        tracker.update(current, 10, 3, "command");

        IgniteInternalException error = assertThrows(IgniteInternalException.class,
                () -> tracker.update(new HybridTimestamp(2, 0), 11, 4, "next-command"));

        assertTrue(error.getMessage().contains("Reordering detected"));
        assertTrue(error.getMessage().contains("oldIndex=10"));
        assertTrue(error.getMessage().contains("oldTerm=3"));
        assertTrue(error.getMessage().contains("oldCommandClassName=java.lang.String"));
        assertEquals(current, tracker.current());
    }

    @Test
    void equalBatchTimestampIsAccepted() {
        SafeTimeValuesTracker tracker = new SafeTimeValuesTracker(new HybridTimestamp(1, 0));
        HybridTimestamp timestamp = new HybridTimestamp(2, 0);

        tracker.update(timestamp, 10, 3, "first-command");
        tracker.update(timestamp, 11, 3, "second-command");

        assertEquals(timestamp, tracker.current());
    }

    @Test
    void closedTrackerRejectsUpdatesAndCompletesWaitersExceptionally() {
        SafeTimeValuesTracker tracker = new SafeTimeValuesTracker(new HybridTimestamp(1, 0));
        CompletableFuture<Void> waiter = tracker.waitFor(new HybridTimestamp(2, 0));

        tracker.close();

        assertThat(waiter, willThrowFast(TrackerClosedException.class));
        assertThrows(TrackerClosedException.class, () -> tracker.update(new HybridTimestamp(2, 0), 10, 3, "command"));
        assertThrows(TrackerClosedException.class, () -> tracker.update(new HybridTimestamp(2, 0), null));
    }
}
