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

package org.apache.ignite.internal.placementdriver;

import java.util.Map;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSetBuilder;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/** Low-cardinality observation and migration metrics, evaluated without network requests. */
final class PrimaryDistributionMetricSource implements MetricSource {
    static final String NAME = "primary-distribution";

    private final Map<String, LongSupplier> values;
    private boolean enabled;

    PrimaryDistributionMetricSource(Map<String, LongSupplier> values) {
        this.values = values;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        if (enabled) {
            return null;
        }
        MetricSetBuilder builder = new MetricSetBuilder(NAME);
        values.forEach((name, supplier) -> builder.longGauge(name,
                name + " (local process; global counts only on active reporter)", supplier));
        enabled = true;
        return builder.build();
    }

    @Override
    public synchronized void disable() {
        enabled = false;
    }

    @Override
    public synchronized boolean enabled() {
        return enabled;
    }
}
