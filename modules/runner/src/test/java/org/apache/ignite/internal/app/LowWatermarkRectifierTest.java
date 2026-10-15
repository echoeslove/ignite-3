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

package org.apache.ignite.internal.app;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests startup rectification against the earliest retained catalog. */
@ExtendWith(MockitoExtension.class)
class LowWatermarkRectifierTest extends BaseIgniteAbstractTest {
    @Mock
    private LowWatermark lowWatermark;

    @Mock
    private CatalogService catalogService;

    @Mock
    private Catalog earliestCatalog;

    @ParameterizedTest
    @NullSource
    @ValueSource(longs = {100, 101})
    void preservesNullOrSufficientWatermark(Long physicalTime) {
        when(earliestCatalog.time()).thenReturn(new HybridTimestamp(100, 0).longValue());
        when(catalogService.earliestCatalog()).thenReturn(earliestCatalog);
        when(lowWatermark.getLowWatermark()).thenReturn(physicalTime == null ? null : new HybridTimestamp(physicalTime, 0));

        assertThat(new LowWatermarkRectifier(lowWatermark, catalogService).startAsync(new ComponentContext()),
                willCompleteSuccessfully());

        verify(lowWatermark, never()).setLowWatermarkOnRecovery(any());
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 99})
    void raisesStaleWatermarkToEarliestCatalog(Long physicalTime) {
        HybridTimestamp earliestTimestamp = new HybridTimestamp(100, 0);
        when(earliestCatalog.time()).thenReturn(earliestTimestamp.longValue());
        when(catalogService.earliestCatalog()).thenReturn(earliestCatalog);
        when(lowWatermark.getLowWatermark()).thenReturn(new HybridTimestamp(physicalTime, 0));

        assertThat(new LowWatermarkRectifier(lowWatermark, catalogService).startAsync(new ComponentContext()),
                willCompleteSuccessfully());

        verify(lowWatermark).setLowWatermarkOnRecovery(earliestTimestamp);
    }
}
