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
import java.util.List;
import java.util.Map;

/** LeaseBalancePlan management value. */
public class LeaseBalancePlan {
    /** planId value. */
    public final String planId;

    /** createdAt value. */
    public final long createdAt;

    /** expiresAt value. */
    public final long expiresAt;

    /** zoneId value. */
    public final int zoneId;

    /** maxMoves value. */
    public final int maxMoves;

    /** reason value. */
    public final String reason;

    /** before value. */
    public final Map<String, Integer> before;

    /** after value. */
    public final Map<String, Integer> after;

    /** moves value. */
    public final List<LeaseMove> moves;

    /** skipped value. */
    public final Map<String, String> skipped;

    /** limitedByMaxMoves value. */
    public final boolean limitedByMaxMoves;

    /** balanced value. */
    public final boolean balanced;

    /** Creates a management value. */
    @JsonCreator
    public LeaseBalancePlan(
            @JsonProperty("planId") String planId,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("expiresAt") long expiresAt,
            @JsonProperty("zoneId") int zoneId,
            @JsonProperty("maxMoves") int maxMoves,
            @JsonProperty("reason") String reason,
            @JsonProperty("before") Map<String, Integer> before,
            @JsonProperty("after") Map<String, Integer> after,
            @JsonProperty("moves") List<LeaseMove> moves,
            @JsonProperty("skipped") Map<String, String> skipped,
            @JsonProperty("limitedByMaxMoves") boolean limitedByMaxMoves,
            @JsonProperty("balanced") boolean balanced
    ) {
        this.planId = planId;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.zoneId = zoneId;
        this.maxMoves = maxMoves;
        this.reason = reason;
        this.before = before;
        this.after = after;
        this.moves = moves;
        this.skipped = skipped;
        this.limitedByMaxMoves = limitedByMaxMoves;
        this.balanced = balanced;
    }
}
