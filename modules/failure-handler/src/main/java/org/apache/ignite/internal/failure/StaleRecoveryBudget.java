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

package org.apache.ignite.internal.failure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/**
 * Durable, fail-closed exit budget shared by all launches using the same work directory.
 * Deliberately no time-based reset: elapsed time or joining topology does not prove replica health.
 * Reset requires an operator to verify stable membership, replicas and data with the node stopped.
 */
public final class StaleRecoveryBudget {
    private final Path directory;

    private final int limit;

    /** Creates a budget without resetting any state from earlier launches. */
    public StaleRecoveryBudget(Path directory, int limit) {
        if (limit < 1 || limit > 100) {
            throw new IllegalArgumentException("Stale recovery limit must be between 1 and 100");
        }
        this.directory = directory.toAbsolutePath().normalize();
        this.limit = limit;
    }

    /** Atomically consumes an attempt before exit. Returns -1 when exhausted; any I/O error denies recovery. */
    public int reserve() throws IOException {
        Files.createDirectories(directory);
        Path lock = directory.resolve("budget.lock");
        Path state = directory.resolve("budget.v1");
        if (Files.isSymbolicLink(directory) || Files.isSymbolicLink(lock) || Files.isSymbolicLink(state)) {
            throw new IOException("Symlink in stale recovery budget path");
        }

        try (FileChannel channel = FileChannel.open(lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS); FileLock ignored = channel.tryLock()) {
            if (ignored == null) {
                throw new IOException("Another launch owns the stale recovery budget");
            }
            int attempts = 0;
            if (Files.exists(state, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.size(state) > 128) {
                    throw new IOException("Oversized stale recovery budget");
                }
                String value = Files.readString(state, StandardCharsets.UTF_8);
                if (!value.matches("stale-recovery-v1\\n[0-9]{1,9}\\n")) {
                    throw new IOException("Corrupt stale recovery budget; manual investigation required");
                }
                attempts = Integer.parseInt(value.split("\\n")[1]);
            }
            if (attempts >= limit) {
                return -1;
            }
            int reserved = attempts + 1;
            Path temporary = directory.resolve("budget-" + UUID.randomUUID() + ".tmp");
            byte[] bytes = ("stale-recovery-v1\n" + reserved + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    output.write(buffer);
                }
                output.force(true);
            }
            Files.move(temporary, state, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel dir = FileChannel.open(directory, StandardOpenOption.READ)) {
                dir.force(true);
            }
            return reserved;
        }
    }
}
