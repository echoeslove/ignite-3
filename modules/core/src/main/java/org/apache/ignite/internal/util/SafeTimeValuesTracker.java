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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Map;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Safe time values tracker. Checks for reordering. Must be updated from a single thread.
 */
public class SafeTimeValuesTracker extends PendingComparableValuesTracker<HybridTimestamp, Void> {
    /** Index of the last command that successfully updated the safe timestamp. */
    private long commandIndex;

    /** Term of the last command that successfully updated the safe timestamp. */
    private long commandTerm;

    /** Class name of the last command, or {@code null} when no command context was supplied. */
    private @Nullable String commandClassName;

    public SafeTimeValuesTracker(HybridTimestamp initialValue) {
        super(initialValue);
    }

    @Override
    public void update(HybridTimestamp newValue, @Nullable Void futureResult) {
        update(newValue, 0, 0, null);
    }

    /**
     * Updates the safe timestamp and records the command context for reordering diagnostics.
     *
     * @param safeTs New safe timestamp.
     * @param commandIndex Command index, or zero when no command context is available.
     * @param commandTerm Command term, or zero when no command context is available.
     * @param command Command, or {@code null} when no command context is available.
     */
    public void update(HybridTimestamp safeTs, long commandIndex, long commandTerm, @Nullable Object command) {
        if (!enterBusy()) {
            throw new TrackerClosedException();
        }

        try {
            Map.Entry<HybridTimestamp, @Nullable Void> current = this.current;

            IgniteBiTuple<HybridTimestamp, @Nullable Void> newEntry = new IgniteBiTuple<>(safeTs, null);

            // Entries from the same batch receive equal safe timestamps.
            if (comparator.compare(newEntry, current) < 0) {
                throw new IgniteInternalException(INTERNAL_ERR,
                        "Reordering detected: [old=" + current.getKey() + ", new=" + newEntry.get1()
                                + ", oldIndex=" + this.commandIndex
                                + ", oldTerm=" + this.commandTerm
                                + ", oldCommandClassName=" + this.commandClassName
                                + ']');
            }

            CURRENT.set(this, newEntry);

            this.commandIndex = commandIndex;
            this.commandTerm = commandTerm;
            this.commandClassName = command == null ? null : command.getClass().getName();

            // Ignite 3.1 also uses this tracker to wait for safe time; keep those readers notified.
            completeWaitersOnUpdate(safeTs, null);
        } finally {
            leaveBusy();
        }
    }
}
