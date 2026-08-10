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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.deployunit.InitialDeployMode.MAJORITY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jdk.jfr.Recording;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.apache.ignite.internal.deployunit.StreamDeploymentUnit;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.SqlException;
import org.example.compute.ClassLoaderMarkerJob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration tests for Compute functionality in standalone Ignite node.
 */
@SuppressWarnings("ThrowableNotThrown")
class ItComputeTestStandalone extends ItComputeBaseTest {
    private final DeploymentUnit unit = new DeploymentUnit("jobs", Version.parseVersion("1.0.0"));

    private final List<DeploymentUnit> units = List.of(unit);

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[] { 0, 1 }; // Majority will be 0, 1
    }

    @BeforeEach
    void deploy() throws IOException {
        sql("CREATE TABLE CLASSLOADER_VALIDATION (id INT PRIMARY KEY, payload VARCHAR(128), category INT)");
        sql("INSERT INTO CLASSLOADER_VALIDATION VALUES (1, 'payload-000001', 1)");
        deployJar(unit.name(), unit.version(), "ignite-integration-test-jobs-1.0-SNAPSHOT.jar");
    }

    @AfterEach
    void undeploy() {
        IgniteDeployment deployment = deployment(0);

        try {
            deployment.undeployAsync(unit.name(), unit.version()).join();
        } catch (Exception ignored) {
            // ignored
        }
        await().until(
                () -> deployment.clusterStatusAsync(unit.name(), unit.version()),
                willBe(nullValue())
        );
    }

    @Override
    protected List<DeploymentUnit> units() {
        return units;
    }

    @Test
    void executeMultipleRemoteJobs() {
        CompletableFuture<Set<String>> majority = unwrapIgniteImpl(node(0)).clusterManagementGroupManager().majority();
        assertThat(majority, will(contains(node(0).name(), node(1).name())));

        assertThat(deployment(0).nodeStatusAsync(unit.name(), unit.version()), willBe(DEPLOYED));
        assertThat(deployment(1).nodeStatusAsync(unit.name(), unit.version()), willBe(oneOf(UPLOADING, DEPLOYED)));
        assertThat(deployment(2).nodeStatusAsync(unit.name(), unit.version()), willBe(nullValue()));

        // Execute concurrently on majority, unit should be in the uploading or deployed state at this point
        List<CompletableFuture<String>> resultsMajority = IntStream.range(0, 3).mapToObj(i -> compute().executeAsync(
                JobTarget.node(clusterNode(1)),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42
        )).collect(Collectors.toList());

        // Execute concurrently on non-majority, unit is missing, will trigger on-demand deploy
        List<CompletableFuture<String>> resultsMissing = IntStream.range(0, 3).mapToObj(i -> compute().executeAsync(
                JobTarget.node(clusterNode(2)),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42
        )).collect(Collectors.toList());

        assertThat(resultsMajority, everyItem(will(equalTo("42"))));
        assertThat(resultsMissing, everyItem(will(equalTo("42"))));
    }

    @Test
    void reusesClassLoaderForSequentialJobs() {
        JobDescriptor<Void, String> job = JobDescriptor.builder(ClassLoaderMarkerJob.class).units(units).build();

        String firstMarker = compute().execute(JobTarget.node(clusterNode(0)), job, null);
        String secondMarker = compute().execute(JobTarget.node(clusterNode(0)), job, null);

        assertEquals(firstMarker, secondMarker);
    }

    @Test
    void classLoaderMarkerJobFailsWhenValidationTableIsMissing() {
        JobDescriptor<Void, String> job = JobDescriptor.builder(ClassLoaderMarkerJob.class).units(units).build();

        sql("DROP TABLE CLASSLOADER_VALIDATION");

        assertThat(
                compute().executeAsync(JobTarget.node(clusterNode(0)), job, null),
                willThrow(SqlException.class)
        );
    }

    @Test
    void doesNotReuseClassLoaderWhenCacheIsDisabled() throws ReflectiveOperationException {
        JobContextManager jobContextManager = IgniteTestUtils.getFieldValue(
                unwrapIgniteImpl(node(0)),
                "computeComponent",
                "jobContextManager"
        );
        Object classLoaderCache = IgniteTestUtils.getFieldValue(jobContextManager, "classLoaderCache");
        int originalMaxIdleEntries = IgniteTestUtils.getFieldValue(classLoaderCache, "maxIdleEntries");
        Method updateMaxIdleEntries = classLoaderCache.getClass().getDeclaredMethod("updateMaxIdleEntries", int.class);

        updateMaxIdleEntries.setAccessible(true);
        updateMaxIdleEntries.invoke(classLoaderCache, 0);

        try {
            JobDescriptor<Void, String> job = JobDescriptor.builder(ClassLoaderMarkerJob.class).units(units).build();

            String firstMarker = compute().execute(JobTarget.node(clusterNode(0)), job, null);
            String secondMarker = compute().execute(JobTarget.node(clusterNode(0)), job, null);

            assertNotEquals(firstMarker, secondMarker);
        } finally {
            updateMaxIdleEntries.invoke(classLoaderCache, originalMaxIdleEntries);
        }
    }

    @Test
    void undeploysUnitWithIdleCachedClassLoader() {
        JobDescriptor<Void, String> job = JobDescriptor.builder(ClassLoaderMarkerJob.class).units(units).build();

        compute().execute(JobTarget.node(clusterNode(0)), job, null);

        IgniteDeployment deployment = deployment(0);

        assertThat(deployment.undeployAsync(unit.name(), unit.version()), willCompleteSuccessfully());
        await().until(
                () -> deployment.clusterStatusAsync(unit.name(), unit.version()),
                willBe(nullValue())
        );
    }

    @Test
    void releasesCachedLoaderAndExecutesAfterNodeRestart() {
        JobDescriptor<Void, String> job = JobDescriptor.builder(ClassLoaderMarkerJob.class).units(units).build();
        String markerBeforeRestart = compute().execute(JobTarget.node(clusterNode(0)), job, null);

        CLUSTER.restartNode(0);

        String markerAfterRestart = compute().execute(JobTarget.node(clusterNode(0)), job, null);

        assertNotEquals(markerBeforeRestart, markerAfterRestart);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "IGNITE_METASPACE_VALIDATION", matches = "true")
    void validatesMetaspaceUnderSequentialLoad() throws Exception {
        int maxIdleEntries = requiredNonNegativeIntEnvironment("IGNITE_METASPACE_VALIDATION_CACHE_ENTRIES");
        int iterations = positiveIntEnvironment("IGNITE_METASPACE_VALIDATION_ITERATIONS", 1_000);
        int warmupIterations = positiveIntEnvironment("IGNITE_METASPACE_VALIDATION_WARMUP", 100);
        long postWorkloadWaitMillis = nonNegativeLongEnvironment("IGNITE_METASPACE_VALIDATION_POST_WORKLOAD_WAIT_MILLIS", 0);
        String runId = System.getenv().getOrDefault("IGNITE_METASPACE_VALIDATION_RUN_ID", "manual");
        Path resultPath = requiredPathEnvironment("IGNITE_METASPACE_VALIDATION_RESULT");
        Path jfrPath = requiredPathEnvironment("IGNITE_METASPACE_VALIDATION_JFR");
        Map<String, Object> metrics = new LinkedHashMap<>();
        int originalMaxIdleEntries = updateClassLoaderCacheMaxIdleEntries(maxIdleEntries);
        Recording recording = createValidationRecording();
        recording.start();

        try {
            JobDescriptor<Void, String> job = JobDescriptor.builder(ClassLoaderMarkerJob.class).units(units).build();

            runMarkerJobs(job, warmupIterations, null);
            forceClassUnloading();

            ClassLoadingMXBean classLoading = ManagementFactory.getClassLoadingMXBean();
            long baselineMetaspace = metaspaceUsedBytes();
            long baselineLoadedClasses = classLoading.getTotalLoadedClassCount();
            long baselineUnloadedClasses = classLoading.getUnloadedClassCount();
            long baselineGcCount = gcCollectionCount();
            long baselineGcTimeMillis = gcCollectionTimeMillis();
            long baselineCpuNanos = processCpuTimeNanos();
            long[] latencyNanos = new long[iterations];

            long workloadStartedNanos = System.nanoTime();
            Set<String> markers = runMarkerJobs(job, iterations, latencyNanos);
            long workloadElapsedNanos = System.nanoTime() - workloadStartedNanos;
            long metaspaceBeforeGc = metaspaceUsedBytes();

            Thread.sleep(postWorkloadWaitMillis);
            forceClassUnloading();

            long metaspaceAfterWorkloadGc = metaspaceUsedBytes();
            long loadedClasses = classLoading.getTotalLoadedClassCount() - baselineLoadedClasses;
            long unloadedClasses = classLoading.getUnloadedClassCount() - baselineUnloadedClasses;
            long gcCount = gcCollectionCount() - baselineGcCount;
            long gcTimeMillis = gcCollectionTimeMillis() - baselineGcTimeMillis;
            long cpuNanos = baselineCpuNanos < 0 ? -1 : processCpuTimeNanos() - baselineCpuNanos;

            long undeployStartedNanos = System.nanoTime();
            assertThat(deployment(0).undeployAsync(unit.name(), unit.version()), willCompleteSuccessfully());
            await().until(() -> deployment(0).clusterStatusAsync(unit.name(), unit.version()), willBe(nullValue()));
            long undeployElapsedNanos = System.nanoTime() - undeployStartedNanos;

            forceClassUnloading();
            long metaspaceAfterUndeployGc = metaspaceUsedBytes();

            deployJar(unit.name(), unit.version(), "ignite-integration-test-jobs-1.0-SNAPSHOT.jar");
            String recoveryMarker = compute().execute(JobTarget.node(clusterNode(0)), job, null);
            boolean recoverySucceeded = recoveryMarker != null && !recoveryMarker.isEmpty();

            recording.stop();

            Arrays.sort(latencyNanos);
            metrics.put("runId", runId);
            metrics.put("javaVersion", System.getProperty("java.version"));
            metrics.put("javaVendor", System.getProperty("java.vendor"));
            metrics.put("clusterNodes", 3);
            metrics.put("cacheMaxIdleEntries", maxIdleEntries);
            metrics.put("warmupIterations", warmupIterations);
            metrics.put("iterations", iterations);
            metrics.put("postWorkloadWaitMillis", postWorkloadWaitMillis);
            metrics.put("uniqueClassLoaderMarkers", markers.size());
            metrics.put("workloadElapsedMillis", nanosToMillis(workloadElapsedNanos));
            metrics.put("throughputJobsPerSecond", iterations * 1_000_000_000.0 / workloadElapsedNanos);
            metrics.put("latencyP50Millis", nanosToMillis(percentile(latencyNanos, 0.50)));
            metrics.put("latencyP95Millis", nanosToMillis(percentile(latencyNanos, 0.95)));
            metrics.put("latencyP99Millis", nanosToMillis(percentile(latencyNanos, 0.99)));
            metrics.put("metaspaceBaselineBytes", baselineMetaspace);
            metrics.put("metaspaceBeforeGcBytes", metaspaceBeforeGc);
            metrics.put("metaspaceAfterWorkloadGcBytes", metaspaceAfterWorkloadGc);
            metrics.put("metaspaceAfterUndeployGcBytes", metaspaceAfterUndeployGc);
            metrics.put("metaspacePeakDeltaBytes", metaspaceBeforeGc - baselineMetaspace);
            metrics.put("metaspaceRetainedAfterWorkloadGcBytes", metaspaceAfterWorkloadGc - baselineMetaspace);
            metrics.put("metaspaceRetainedAfterUndeployGcBytes", metaspaceAfterUndeployGc - baselineMetaspace);
            metrics.put("loadedClasses", loadedClasses);
            metrics.put("unloadedClasses", unloadedClasses);
            metrics.put("gcCount", gcCount);
            metrics.put("gcTimeMillis", gcTimeMillis);
            metrics.put("processCpuMillis", nanosToMillis(cpuNanos));
            metrics.put("undeployMillis", nanosToMillis(undeployElapsedNanos));
            metrics.put("recoverySucceeded", recoverySucceeded);
            metrics.put("errors", 0);

            Files.createDirectories(resultPath.getParent());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(resultPath.toFile(), metrics);

            if (maxIdleEntries == 0) {
                assertEquals(iterations, markers.size(), "Disabled cache must create a loader for every sequential job");
            } else {
                assertEquals(1, markers.size(), "Enabled cache must reuse one loader for sequential jobs");
            }
            assertTrue(recoverySucceeded, "Compute must recover after undeploy and redeploy");
        } finally {
            if (recording.getState() == jdk.jfr.RecordingState.RUNNING) {
                recording.stop();
            }

            Files.createDirectories(jfrPath.getParent());
            recording.dump(jfrPath);
            recording.close();
            updateClassLoaderCacheMaxIdleEntries(originalMaxIdleEntries);
        }
    }

    @Test
    void executesJobWithNonExistingUnit() {
        List<DeploymentUnit> nonExistingUnits = List.of(new DeploymentUnit("non-existing", "1.0.0"));
        CompletableFuture<String> result = compute().executeAsync(
                JobTarget.node(clusterNode(0)),
                JobDescriptor.builder(toStringJobClass()).units(nonExistingUnits).build(),
                null
        );

        assertThat(result, willThrow(computeJobFailedException(
                ClassNotFoundException.class.getName(),
                "org.apache.ignite.internal.compute.ToStringJob. Deployment unit non-existing:1.0.0 doesn't exist"
        )));
    }

    @Test
    void executeJobWithoutUnit() {
        IgniteTestUtils.assertThrows(
                ComputeException.class,
                () -> compute().execute(
                        JobTarget.node(clusterNode(0)),
                        JobDescriptor.builder("org.apache.ignite.internal.compute.UnitJob").build(),
                        null
                ),
                "Cannot load job class by name 'org.apache.ignite.internal.compute.UnitJob'."
                        + " Deployment units list is empty.");
    }

    @Test
    void executeTaskWithoutUnit() {
        IgniteTestUtils.assertThrows(
                ComputeException.class,
                () -> compute().executeMapReduce(
                        TaskDescriptor.builder("org.apache.ignite.internal.compute.UnitJob").build(),
                        null
                ),
                "Cannot load task class by name 'org.apache.ignite.internal.compute.UnitJob'."
                        + " Deployment units list is empty.");
    }

    @Test
    void executesJobWithLatestUnitVersion() throws IOException {
        List<DeploymentUnit> jobUnits = List.of(new DeploymentUnit("latest-unit", Version.LATEST));

        DeploymentUnit firstVersion = new DeploymentUnit("latest-unit", Version.parseVersion("1.0.0"));
        deployJar(firstVersion.name(), firstVersion.version(), "ignite-unit-test-job1-1.0-SNAPSHOT.jar");

        JobDescriptor job = JobDescriptor.builder("org.apache.ignite.internal.compute.UnitJob").units(jobUnits).build();
        CompletableFuture<Integer> result1 = compute().executeAsync(JobTarget.node(clusterNode(0)), job, null);
        assertThat(result1, willBe(1));

        DeploymentUnit secondVersion = new DeploymentUnit("latest-unit", Version.parseVersion("1.0.1"));
        deployJar(secondVersion.name(), secondVersion.version(), "ignite-unit-test-job2-1.0-SNAPSHOT.jar");

        CompletableFuture<String> result2 = compute().executeAsync(JobTarget.node(clusterNode(0)), job, null);
        assertThat(result2, willBe("Hello World!"));
    }

    @Test
    void undeployAcquiredUnit() {
        CompletableFuture<Void> job = compute().executeAsync(
                JobTarget.node(clusterNode(0)),
                JobDescriptor.builder(SleepJob.class).units(units).build(),
                3L);

        IgniteDeployment deployment = deployment(0);

        assertThat(deployment.undeployAsync(unit.name(), unit.version()), willCompleteSuccessfully());

        assertThat(deployment.clusterStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));
        assertThat(deployment.nodeStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));

        await().failFast("The unit must not be removed until the job is completed", () -> {
            assertThat(deployment.clusterStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));
            assertThat(deployment.nodeStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));
        }).until(() -> job, willCompleteSuccessfully());

        await().until(
                () -> deployment.clusterStatusAsync(unit.name(), unit.version()),
                willBe(nullValue())
        );
    }

    @Test
    void executeJobWithObsoleteUnit() {
        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units).build();

        CompletableFuture<Void> successJob = compute().executeAsync(JobTarget.node(clusterNode(0)), job, 2L);

        assertThat(deployment(0).undeployAsync(unit.name(), unit.version()), willCompleteSuccessfully());

        CompletableFuture<Void> failedJob = compute().executeAsync(JobTarget.node(clusterNode(0)), job, 2L);

        assertThat(failedJob, willThrow(computeJobFailedException(
                ClassNotFoundException.class.getName(),
                "Deployment unit jobs:1.0.0 can't be used: [clusterStatus = OBSOLETE, nodeStatus = OBSOLETE]"
        )));

        assertThat(successJob, willCompleteSuccessfully());
    }

    @Test
    void jobContextProvidesDeploymentUnitInfo() {
        var units0 = List.of(
                new DeploymentUnit(unit.name(), Version.LATEST),
                unit);

        JobDescriptor<Void, String> job = JobDescriptor.builder(DeploymentUnitInfoJob.class).units(units0).build();

        String jobRes = compute().execute(JobTarget.node(clusterNode(0)), job, null);

        assertThat(jobRes, containsString("name='jobs'"));
        assertThat(jobRes, containsString("version=1.0.0"));
        assertThat(jobRes, containsString("path="));
        assertThat(jobRes.split(";").length, equalTo(2));
    }

    @Test
    void jobContextProvidesDeploymentUnitPath() {
        JobDescriptor<Void, String> job = JobDescriptor.builder(DeploymentUnitContentJob.class).units(units).build();

        String jobRes = compute().execute(JobTarget.node(clusterNode(0)), job, null);

        assertEquals("ignite-integration-test-jobs-1.0-SNAPSHOT.jar", jobRes);
    }

    private static void deployJar(String unitId, Version unitVersion, String jarName) throws IOException {
        IgniteDeployment deployment = deployment(0);

        try (InputStream jarStream = ItComputeTestStandalone.class.getClassLoader().getResourceAsStream("units/" + jarName)) {
            CompletableFuture<Boolean> deployed = deployment.deployAsync(
                    unitId,
                    unitVersion,
                    new StreamDeploymentUnit(Map.of(jarName, jarStream)),
                    new NodesToDeploy(MAJORITY)
            );

            assertThat(deployed, willBe(true));
            await().until(() -> deployment.clusterStatusAsync(unitId, unitVersion), willBe(DEPLOYED));
        }
    }

    private static IgniteDeployment deployment(int nodeIndex) {
        return unwrapIgniteImpl(node(nodeIndex)).deployment();
    }

    private static int updateClassLoaderCacheMaxIdleEntries(int maxIdleEntries) throws ReflectiveOperationException {
        JobContextManager jobContextManager = IgniteTestUtils.getFieldValue(
                unwrapIgniteImpl(node(0)),
                "computeComponent",
                "jobContextManager"
        );
        Object classLoaderCache = IgniteTestUtils.getFieldValue(jobContextManager, "classLoaderCache");
        int previousMaxIdleEntries = IgniteTestUtils.getFieldValue(classLoaderCache, "maxIdleEntries");
        Method updateMaxIdleEntries = classLoaderCache.getClass().getDeclaredMethod("updateMaxIdleEntries", int.class);

        updateMaxIdleEntries.setAccessible(true);
        updateMaxIdleEntries.invoke(classLoaderCache, maxIdleEntries);

        return previousMaxIdleEntries;
    }

    private Set<String> runMarkerJobs(
            JobDescriptor<Void, String> job,
            int iterations,
            long[] latencyNanos
    ) {
        java.util.HashSet<String> markers = new java.util.HashSet<>();

        for (int i = 0; i < iterations; i++) {
            long startedNanos = System.nanoTime();
            markers.add(compute().execute(JobTarget.node(clusterNode(0)), job, null));

            if (latencyNanos != null) {
                latencyNanos[i] = System.nanoTime() - startedNanos;
            }
        }

        return markers;
    }

    private static Recording createValidationRecording() {
        Recording recording = new Recording();
        recording.setName("ignite-job-classloader-cache-validation");
        recording.enable("jdk.ClassLoad");
        recording.enable("jdk.ClassUnload");
        recording.enable("jdk.GarbageCollection");
        recording.enable("jdk.MetaspaceSummary");
        recording.enable("jdk.CPULoad").withPeriod(java.time.Duration.ofSeconds(1));
        return recording;
    }

    private static void forceClassUnloading() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(200);
        }
    }

    private static long metaspaceUsedBytes() {
        return ManagementFactory.getMemoryPoolMXBeans().stream()
                .filter(pool -> "Metaspace".equals(pool.getName()))
                .map(MemoryPoolMXBean::getUsage)
                .mapToLong(usage -> usage.getUsed())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Metaspace memory pool is not available"));
    }

    private static long gcCollectionCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                .filter(value -> value >= 0)
                .sum();
    }

    private static long gcCollectionTimeMillis() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                .filter(value -> value >= 0)
                .sum();
    }

    private static long processCpuTimeNanos() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();

        return bean instanceof com.sun.management.OperatingSystemMXBean
                ? ((com.sun.management.OperatingSystemMXBean) bean).getProcessCpuTime()
                : -1;
    }

    private static long percentile(long[] sortedValues, double percentile) {
        int index = (int) Math.ceil(percentile * sortedValues.length) - 1;
        return sortedValues[Math.max(0, index)];
    }

    private static double nanosToMillis(long nanos) {
        return nanos < 0 ? -1 : nanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
    }

    private static int requiredNonNegativeIntEnvironment(String name) {
        String value = System.getenv(name);

        if (value == null) {
            throw new IllegalArgumentException("Missing environment variable: " + name);
        }

        int parsed = Integer.parseInt(value);

        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }

        return parsed;
    }

    private static int positiveIntEnvironment(String name, int defaultValue) {
        int parsed = Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(defaultValue)));

        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }

        return parsed;
    }

    private static long nonNegativeLongEnvironment(String name, long defaultValue) {
        long parsed = Long.parseLong(System.getenv().getOrDefault(name, Long.toString(defaultValue)));

        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }

        return parsed;
    }

    private static Path requiredPathEnvironment(String name) {
        String value = System.getenv(name);

        if (value == null) {
            throw new IllegalArgumentException("Missing environment variable: " + name);
        }

        return Path.of(value).toAbsolutePath();
    }
}
