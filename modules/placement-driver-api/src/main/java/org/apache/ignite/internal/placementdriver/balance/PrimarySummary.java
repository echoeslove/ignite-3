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

/** PrimarySummary management value. */
public class PrimarySummary {
    /** observedAt value. */
    public final long observedAt;

    /** catalogVersion value. */
    public final int catalogVersion;

    /** topologyVersion value. */
    public final long topologyVersion;

    /** leaseRevision value. */
    public final long leaseRevision;

    /** freshness value. */
    public final String freshness;

    /** complete value. */
    public final boolean complete;

    /** replicationMode value. */
    public final String replicationMode;

    /** totalGroups value. */
    public final int totalGroups;

    /** withoutPrimary value. */
    public final int withoutPrimary;

    /** nodes value. */
    public final List<PrimaryNode> nodes;

    /** zones value. */
    public final Map<Integer, Map<String, Integer>> zones;

    /** Creates a management value. */
    @JsonCreator
    public PrimarySummary(
            @JsonProperty("observedAt") long observedAt,
            @JsonProperty("catalogVersion") int catalogVersion,
            @JsonProperty("topologyVersion") long topologyVersion,
            @JsonProperty("leaseRevision") long leaseRevision,
            @JsonProperty("freshness") String freshness,
            @JsonProperty("complete") boolean complete,
            @JsonProperty("replicationMode") String replicationMode,
            @JsonProperty("totalGroups") int totalGroups,
            @JsonProperty("withoutPrimary") int withoutPrimary,
            @JsonProperty("nodes") List<PrimaryNode> nodes,
            @JsonProperty("zones") Map<Integer, Map<String, Integer>> zones
    ) {
        this.observedAt = observedAt;
        this.catalogVersion = catalogVersion;
        this.topologyVersion = topologyVersion;
        this.leaseRevision = leaseRevision;
        this.freshness = freshness;
        this.complete = complete;
        this.replicationMode = replicationMode;
        this.totalGroups = totalGroups;
        this.withoutPrimary = withoutPrimary;
        this.nodes = nodes;
        this.zones = zones;
    }
}
