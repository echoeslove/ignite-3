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

package org.apache.ignite.internal.rest.api.distribution;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalanceOperation;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlan;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlanRequest;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalanceRequest;
import org.apache.ignite.internal.placementdriver.balance.PrimaryPage;
import org.apache.ignite.internal.placementdriver.balance.PrimarySummary;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;
import org.jetbrains.annotations.Nullable;

/** Primary lease observation and manually requested balancing. */
@Controller("/management/v1/distribution")
@Tag(name = "distribution")
public interface DistributionApi {
    /** Summarizes effective Primary lease holders. */
    @Get("/primary-replicas/summary")
    @Operation(operationId = "primaryDistributionSummary", summary = "Observe Primary distribution")
    CompletableFuture<PrimarySummary> summary(@Nullable @QueryValue Integer zoneId);

    /** Reads a stable, short-lived metadata snapshot. */
    @Get("/primary-replicas")
    @Operation(operationId = "primaryDistributionDetails", summary = "List Primary replica leases")
    CompletableFuture<PrimaryPage> replicas(@Nullable @QueryValue Integer zoneId, @Nullable @QueryValue String node,
            @Nullable @QueryValue String state, @Nullable @QueryValue String snapshotId,
            @QueryValue(defaultValue = "0") int offset, @QueryValue(defaultValue = "100") int limit);

    /** Previews a plan without changing leases. */
    @Post("/balance-leases/plan")
    @Operation(operationId = "planLeaseBalance", summary = "Preview a manual Primary balance plan")
    CompletableFuture<LeaseBalancePlan> plan(@Body LeaseBalancePlanRequest request);

    /** Accepts a durable operation; completion must be checked using its operationId. */
    @Post("/balance-leases")
    @Operation(operationId = "executeLeaseBalance", summary = "Accept a manual Primary balance operation")
    @ApiResponse(responseCode = "202", description = "Manual balance operation accepted.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = LeaseBalanceOperation.class)))
    @ApiResponse(responseCode = "400", description = "Invalid balance request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "409", description = "Balancing disabled, plan stale, or another operation unresolved.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    CompletableFuture<HttpResponse<LeaseBalanceOperation>> execute(@Body LeaseBalanceRequest request);

    /** Returns actual migration progress, including unresolved timeouts. */
    @Get("/balance-leases/operations/{operationId}")
    @Operation(operationId = "leaseBalanceOperation", summary = "Observe a manual Primary balance operation")
    CompletableFuture<LeaseBalanceOperation> operation(String operationId);
}
