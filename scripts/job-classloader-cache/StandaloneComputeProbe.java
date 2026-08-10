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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.network.ClusterNode;

/** Standalone thin-client probe for the ZIP-deployed class-loader cache validation cluster. */
public class StandaloneComputeProbe {
    private static final String JOB_CLASS_NAME = "org.example.compute.ClassLoaderMarkerJob";

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Usage: workload|missing-unit ...");
        }

        switch (args[0]) {
            case "workload":
                runWorkload(args);
                return;
            case "missing-unit":
                runMissingUnitCheck(args);
                return;
            default:
                throw new IllegalArgumentException("Unknown mode: " + args[0]);
        }
    }

    private static void runWorkload(String[] args) {
        if (args.length != 8) {
            throw new IllegalArgumentException(
                    "Usage: workload <addresses> <node> <unitId> <version> <iterations> <parallelism> <expectedDistinct>"
            );
        }

        String[] addresses = args[1].split(",");
        String nodeName = args[2];
        String unitId = args[3];
        String unitVersion = args[4];
        int iterations = Integer.parseInt(args[5]);
        int parallelism = Integer.parseInt(args[6]);
        int expectedDistinct = Integer.parseInt(args[7]);

        if (iterations <= 0 || parallelism <= 0) {
            throw new IllegalArgumentException("iterations and parallelism must be positive");
        }

        long startedNanos = System.nanoTime();
        Set<String> markers = new HashSet<>();

        try (IgniteClient client = IgniteClient.builder().addresses(addresses).build()) {
            ClusterNode targetNode = findNode(client, nodeName);
            JobDescriptor<Void, String> descriptor = descriptor(unitId, unitVersion);

            for (int offset = 0; offset < iterations; offset += parallelism) {
                int batchSize = Math.min(parallelism, iterations - offset);
                List<CompletableFuture<String>> futures = new ArrayList<>(batchSize);

                for (int i = 0; i < batchSize; i++) {
                    futures.add(client.compute().executeAsync(JobTarget.node(targetNode), descriptor, null));
                }

                for (CompletableFuture<String> future : futures) {
                    markers.add(future.join());
                }
            }
        }

        if (markers.size() != expectedDistinct) {
            throw new AssertionError("Expected " + expectedDistinct + " distinct markers, got " + markers.size());
        }

        long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000;
        String firstMarker = markers.iterator().next();
        System.out.printf(
                "RESULT status=PASS node=%s iterations=%d parallelism=%d distinctMarkers=%d firstMarker=%s elapsedMs=%d%n",
                nodeName,
                iterations,
                parallelism,
                markers.size(),
                firstMarker,
                elapsedMillis
        );
    }

    private static void runMissingUnitCheck(String[] args) {
        if (args.length != 6) {
            throw new IllegalArgumentException("Usage: missing-unit <addresses> <node> <unitId> <validVersion> <missingVersion>");
        }

        String[] addresses = args[1].split(",");
        String nodeName = args[2];
        String unitId = args[3];
        String validVersion = args[4];
        String missingVersion = args[5];

        try (IgniteClient client = IgniteClient.builder().addresses(addresses).build()) {
            ClusterNode targetNode = findNode(client, nodeName);
            boolean failedAsExpected = false;

            try {
                client.compute().execute(JobTarget.node(targetNode), descriptor(unitId, missingVersion), null);
            } catch (RuntimeException expected) {
                failedAsExpected = true;
                System.out.println("EXPECTED_FAILURE missingVersion=" + missingVersion + " type=" + expected.getClass().getName());
            }

            if (!failedAsExpected) {
                throw new AssertionError("Execution with missing unit version unexpectedly succeeded");
            }

            String marker = client.compute().execute(JobTarget.node(targetNode), descriptor(unitId, validVersion), null);
            System.out.println("RESULT status=PASS recoveryMarker=" + marker);
        }
    }

    private static ClusterNode findNode(IgniteClient client, String nodeName) {
        return client.cluster().nodes().stream()
                .filter(node -> node.name().equals(nodeName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Node not found in logical topology: " + nodeName));
    }

    private static JobDescriptor<Void, String> descriptor(String unitId, String unitVersion) {
        return JobDescriptor.<Void, String>builder(JOB_CLASS_NAME)
                .units(new DeploymentUnit(unitId, unitVersion))
                .resultClass(String.class)
                .build();
    }
}
