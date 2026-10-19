#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ "$1" = 'cli' ]; then
  shift
  # Override user.home property to keep CLI-produced files in the separate work directory
  IGNITE3_OPTS=-Duser.home=$IGNITE_CLI_WORK_DIR exec sh "$IGNITE_CLI_HOME"/bin/ignite3 "$@"
fi

. @LIB_DIR@/@BOOTSTRAP_FILE_NAME@

LOGGING_JAVA_OPTS="-Djava.util.logging.config.file=@CONF_DIR@/ignite.java.util.logging.properties"

JAVA_MEMORY_OPTIONS="-Xmx${JVM_MAX_MEM} -Xms${JVM_MIN_MEM}"

JAVA_GC_OPTIONS="-XX:+Use${JVM_GC} -XX:G1HeapRegionSize=${JVM_G1HeapRegionSize}"

if [ "${IGNITE_STALE_RECOVERY_ENABLED:-false}" = 'true' ]; then
  IGNITE3_EXTRA_JVM_ARGS="${IGNITE3_EXTRA_JVM_ARGS:-} -Dignite.staleRecovery.enabled=true"
fi

CMD="${JAVACMD} \
  ${COMMON_JAVA_OPTS} \
  ${LOGGING_JAVA_OPTS} \
  ${JAVA_MEMORY_OPTIONS} \
  ${JAVA_GC_OPTIONS} \
  ${IGNITE3_EXTRA_JVM_ARGS} \
  ${CLASSPATH}"

if [ "${IGNITE_STALE_RECOVERY_ENABLED:-false}" != 'true' ]; then
  exec $CMD "$@"
fi

# Opt-in, single-node JVM only. Persist the SAME work mount, including stale-recovery/budget.v1.
# Only the dedicated segmentation exit is restarted; all unrelated failures propagate unchanged.
# The Java side reserves a durable budget and applies backoff BEFORE emitting exit code 75.
recovery_child=''
recovery_stopping=false
trap 'recovery_stopping=true; if [ -n "$recovery_child" ]; then kill -TERM "$recovery_child" 2>/dev/null; fi' TERM INT
while :; do
  if [ "$recovery_stopping" = 'true' ]; then
    exit 143
  fi
  $CMD "$@" &
  recovery_child=$!
  # Also cover a signal arriving between the loop check and publication of the child PID.
  if [ "$recovery_stopping" = 'true' ]; then
    kill -TERM "$recovery_child" 2>/dev/null
    wait "$recovery_child" 2>/dev/null
    exit 143
  fi
  wait "$recovery_child"
  recovery_status=$?
  if [ "$recovery_stopping" = 'true' ]; then
    wait "$recovery_child" 2>/dev/null
    exit 143
  fi
  recovery_child=''
  if [ "$recovery_status" -ne 75 ]; then
    exit "$recovery_status"
  fi
  echo 'STALE_RECOVERY_RELAUNCH: reusing the existing work directory'
done
