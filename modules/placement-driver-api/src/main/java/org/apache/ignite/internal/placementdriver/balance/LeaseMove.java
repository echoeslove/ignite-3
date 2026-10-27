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

/** LeaseMove management value. */
public class LeaseMove {
    /** group value. */
    public final PrimaryGroup group;

    /** target value. */
    public final String target;

    /** targetId value. */
    public final String targetId;

    /** state value. */
    public final String state;

    /** submittedAt value. */
    public final long submittedAt;

    /** oldExpiration value. */
    public final long oldExpiration;

    /** reason value. */
    public final String reason;

    /** Creates a management value. */
    @JsonCreator
    public LeaseMove(
            @JsonProperty("group") PrimaryGroup group,
            @JsonProperty("target") String target,
            @JsonProperty("targetId") String targetId,
            @JsonProperty("state") String state,
            @JsonProperty("submittedAt") long submittedAt,
            @JsonProperty("oldExpiration") long oldExpiration,
            @JsonProperty("reason") String reason
    ) {
        this.group = group;
        this.target = target;
        this.targetId = targetId;
        this.state = state;
        this.submittedAt = submittedAt;
        this.oldExpiration = oldExpiration;
        this.reason = reason;
    }
}
