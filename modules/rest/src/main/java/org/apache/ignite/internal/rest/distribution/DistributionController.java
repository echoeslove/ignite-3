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

package org.apache.ignite.internal.rest.distribution;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.context.ServerRequestContext;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.placementdriver.PrimaryDistributionManager;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalanceOperation;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlan;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlanRequest;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalanceRequest;
import org.apache.ignite.internal.placementdriver.balance.PrimaryPage;
import org.apache.ignite.internal.placementdriver.balance.PrimarySummary;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.distribution.DistributionApi;
import org.jetbrains.annotations.Nullable;

/** REST adapter; all lease mutations are delegated to the fenced Placement Driver path. */
@Controller("/management/v1/distribution")
public class DistributionController implements DistributionApi, ResourceHolder {
    private PrimaryDistributionManager manager;

    /** Creates the adapter. */
    public DistributionController(PrimaryDistributionManager manager) {
        this.manager = manager;
    }

    @Override
    public CompletableFuture<PrimarySummary> summary(@Nullable Integer zoneId) {
        return manager.summary(zoneId);
    }

    @Override
    public CompletableFuture<PrimaryPage> replicas(@Nullable Integer zoneId, @Nullable String node, @Nullable String state,
            @Nullable String snapshotId, int offset, int limit) {
        return manager.page(zoneId, node, state, snapshotId, offset, limit);
    }

    @Override
    public CompletableFuture<LeaseBalancePlan> plan(LeaseBalancePlanRequest request) {
        return manager.plan(request.zoneId, request.maxMoves == 0 ? 16 : request.maxMoves);
    }

    @Override
    public CompletableFuture<HttpResponse<LeaseBalanceOperation>> execute(LeaseBalanceRequest request) {
        String requestedBy = ServerRequestContext.currentRequest().flatMap(http -> http.getUserPrincipal())
                .map(java.security.Principal::getName).orElse("anonymous");
        return manager.execute(request.planId, request.requestId, requestedBy).thenApply(operation -> HttpResponse
                .<LeaseBalanceOperation>accepted().body(operation)
                .headers(headers -> headers.location(URI.create("/management/v1/distribution/balance-leases/operations/"
                        + operation.operationId))));
    }

    @Override
    public CompletableFuture<LeaseBalanceOperation> operation(String operationId) {
        return manager.operation(operationId);
    }

    @Override
    public void cleanResources() {
        manager = null;
    }
}
