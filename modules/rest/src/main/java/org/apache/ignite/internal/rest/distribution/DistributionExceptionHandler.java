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

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.placementdriver.DistributionException;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.HttpCode;

/** Maps explicit management rejections without disguising them as successful triggers. */
@Requires(classes = {DistributionException.class, ExceptionHandler.class})
@Singleton
public class DistributionExceptionHandler implements ExceptionHandler<DistributionException, HttpResponse<?>> {
    @Override
    public HttpResponse<?> handle(HttpRequest request, DistributionException exception) {
        HttpCode status = HttpCode.valueOf(exception.status());
        return HttpResponse.status(io.micronaut.http.HttpStatus.valueOf(exception.status())).contentType("application/problem+json")
                .body(Problem.fromHttpCode(status).detail(exception.getMessage()).build());
    }
}
