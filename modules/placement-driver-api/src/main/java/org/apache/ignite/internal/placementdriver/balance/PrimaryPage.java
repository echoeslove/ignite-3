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

/** PrimaryPage management value. */
public class PrimaryPage {
    /** snapshot value. */
    public final PrimarySnapshot snapshot;

    /** items value. */
    public final List<PrimaryGroup> items;

    /** nextOffset value. */
    public final Integer nextOffset;

    /** Creates a management value. */
    @JsonCreator
    public PrimaryPage(
            @JsonProperty("snapshot") PrimarySnapshot snapshot,
            @JsonProperty("items") List<PrimaryGroup> items,
            @JsonProperty("nextOffset") Integer nextOffset
    ) {
        this.snapshot = snapshot;
        this.items = items;
        this.nextOffset = nextOffset;
    }
}
