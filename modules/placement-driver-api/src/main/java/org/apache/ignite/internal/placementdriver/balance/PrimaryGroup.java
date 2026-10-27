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

/** PrimaryGroup management value. */
public class PrimaryGroup {
    /** groupType value. */
    public final String groupType;

    /** objectId value. */
    public final int objectId;

    /** zoneId value. */
    public final int zoneId;

    /** partitionId value. */
    public final int partitionId;

    /** holder value. */
    public final String holder;

    /** holderId value. */
    public final String holderId;

    /** Full hybrid timestamp token identifying a lease generation, including its logical component. */
    public final long leaseStart;

    /** Physical expiration time in milliseconds since the Unix epoch. */
    public final long leaseExpiration;

    /** accepted value. */
    public final boolean accepted;

    /** prolongable value. */
    public final boolean prolongable;

    /** state value. */
    public final String state;

    /** stablePeers value. */
    public final List<String> stablePeers;

    /** assignmentRevision value. */
    public final long assignmentRevision;

    /** pending value. */
    public final boolean pending;

    /** Creates a management value. */
    @JsonCreator
    public PrimaryGroup(
            @JsonProperty("groupType") String groupType,
            @JsonProperty("objectId") int objectId,
            @JsonProperty("zoneId") int zoneId,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("holder") String holder,
            @JsonProperty("holderId") String holderId,
            @JsonProperty("leaseStart") long leaseStart,
            @JsonProperty("leaseExpiration") long leaseExpiration,
            @JsonProperty("accepted") boolean accepted,
            @JsonProperty("prolongable") boolean prolongable,
            @JsonProperty("state") String state,
            @JsonProperty("stablePeers") List<String> stablePeers,
            @JsonProperty("assignmentRevision") long assignmentRevision,
            @JsonProperty("pending") boolean pending
    ) {
        this.groupType = groupType;
        this.objectId = objectId;
        this.zoneId = zoneId;
        this.partitionId = partitionId;
        this.holder = holder;
        this.holderId = holderId;
        this.leaseStart = leaseStart;
        this.leaseExpiration = leaseExpiration;
        this.accepted = accepted;
        this.prolongable = prolongable;
        this.state = state;
        this.stablePeers = stablePeers;
        this.assignmentRevision = assignmentRevision;
        this.pending = pending;
    }
}
