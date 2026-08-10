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

package org.example.compute;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;

/** Compute job that returns a marker scoped to the class that its job class loader defined. */
public class ClassLoaderMarkerJob implements ComputeJob<Void, String> {
    private static final String CLASS_LOADER_MARKER = randomUUID().toString();

    @Override
    public CompletableFuture<String> executeAsync(JobExecutionContext context, Void input) {
        try (ResultSet<SqlRow> rows = context.ignite().sql().execute(
                null,
                "SELECT payload FROM CLASSLOADER_VALIDATION WHERE id = 1"
        )) {
            if (!rows.hasNext()) {
                throw new IllegalStateException("Validation row not found");
            }

            String payload = rows.next().stringValue(0);

            if (!"payload-000001".equals(payload)) {
                throw new IllegalStateException("Unexpected validation payload: " + payload);
            }
        }

        return completedFuture(CLASS_LOADER_MARKER);
    }
}
