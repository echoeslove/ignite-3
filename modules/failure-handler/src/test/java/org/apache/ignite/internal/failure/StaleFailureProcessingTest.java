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

package org.apache.ignite.internal.failure;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.failure.handlers.FailureHandler;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies the coordinator together with the real failure processor, not just the budget in isolation. */
@ExtendWith(FailureManagerExtension.class)
@MuteFailureManagerLogging
class StaleFailureProcessingTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"exhausted", "corrupt", "unavailable"})
    void deniedRecoveryDoesNotSuppressOrdinaryFailure(String reason) throws Exception {
        Path budget = directory.resolve("budget");
        if (reason.equals("unavailable")) {
            Files.writeString(budget, "not a directory");
        } else {
            Files.createDirectory(budget);
            Files.writeString(budget.resolve("budget.v1"), reason.equals("exhausted") ? "stale-recovery-v1\n3\n" : "corrupt");
        }

        FailureHandler handler = mock(FailureHandler.class);
        when(handler.onFailure(any())).thenReturn(true);
        StaleNodeRecovery recovery = new StaleNodeRecovery(() -> {
            throw new AssertionError("Denied recovery must not stop the node");
        }, budget, 3, 0, 1000, ignored -> {
            throw new AssertionError("Denied recovery must not exit the JVM");
        });
        FailureManager manager = new FailureManager(handler).withStaleNodeRecovery(recovery);
        LogInspector blocked = new LogInspector(StaleNodeRecovery.class.getName(),
                event -> event.getMessage().getFormattedMessage().startsWith("STALE_RECOVERY_BLOCKED"));
        blocked.start();
        try {
            assertFalse(manager.process(new FailureContext(FailureType.CRITICAL_ERROR, new StaleNodeFailureException("stale"))));
            assertTrue(waitForCondition(blocked::isMatched, 5000));
            assertNull(manager.failureContext());
            verify(handler, never()).onFailure(any());

            FailureContext ordinary = new FailureContext(FailureType.CRITICAL_ERROR, new IllegalStateException("ordinary failure"));
            assertTrue(manager.process(ordinary));
            verify(handler).onFailure(ordinary);
            assertSame(ordinary, manager.failureContext());
        } finally {
            manager.stopAsync(new ComponentContext()).join();
            blocked.stop();
        }
    }
}
