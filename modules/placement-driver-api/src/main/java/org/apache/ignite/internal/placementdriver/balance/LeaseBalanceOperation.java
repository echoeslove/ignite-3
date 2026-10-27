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

package org.apache.ignite.internal.placementdriver.balance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/** LeaseBalanceOperation management value. */
public class LeaseBalanceOperation {
    /** formatVersion value. */
    public final int formatVersion;

    /** operationId value. */
    public final String operationId;

    /** requestId value. */
    public final String requestId;

    /** createdAt value. */
    public final long createdAt;

    /** updatedAt value. */
    public final long updatedAt;

    /** ownerTerm value. */
    public final long ownerTerm;

    /** state value. */
    public final String state;

    /** reason value. */
    public final String reason;

    /** plan value. */
    public final LeaseBalancePlan plan;

    /** unresolved value. */
    public final boolean unresolved;

    /** actualDistribution value. */
    public final Map<String, Integer> actualDistribution;

    /** Authenticated principal name, or anonymous/internal when no HTTP principal is available. */
    public final String requestedBy;

    /** Creates an internal management value. */
    public LeaseBalanceOperation(int formatVersion, String operationId, String requestId, long createdAt, long updatedAt,
            long ownerTerm, String state, String reason, LeaseBalancePlan plan, boolean unresolved,
            Map<String, Integer> actualDistribution) {
        this(formatVersion, operationId, requestId, createdAt, updatedAt, ownerTerm, state, reason, plan,
                unresolved, actualDistribution, "internal");
    }

    /** Creates a management value. */
    @JsonCreator
    public LeaseBalanceOperation(
            @JsonProperty("formatVersion") int formatVersion,
            @JsonProperty("operationId") String operationId,
            @JsonProperty("requestId") String requestId,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("updatedAt") long updatedAt,
            @JsonProperty("ownerTerm") long ownerTerm,
            @JsonProperty("state") String state,
            @JsonProperty("reason") String reason,
            @JsonProperty("plan") LeaseBalancePlan plan,
            @JsonProperty("unresolved") boolean unresolved,
            @JsonProperty("actualDistribution") Map<String, Integer> actualDistribution,
            @JsonProperty("requestedBy") String requestedBy
    ) {
        this.formatVersion = formatVersion;
        this.operationId = operationId;
        this.requestId = requestId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.ownerTerm = ownerTerm;
        this.state = state;
        this.reason = reason;
        this.plan = plan;
        this.unresolved = unresolved;
        this.actualDistribution = actualDistribution;
        this.requestedBy = requestedBy == null ? "internal" : requestedBy;
    }
}
