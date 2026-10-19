"""Tests the actual container entrypoint's opt-in supervisor without Docker or an Ignite JVM."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest


REPO = Path(__file__).resolve().parents[2]


class StaleSupervisorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="hf4-stale-supervisor-")
        self.root = Path(self.temporary.name)
        self.log = self.root / "launches.jsonl"
        self.stub = self.root / "fake-java"
        self.stub.write_text("""#!/usr/bin/env python3
import json, os, pathlib, signal, sys, time
log = pathlib.Path(os.environ['LAUNCH_LOG'])
with log.open('a') as stream:
    stream.write(json.dumps(sys.argv[1:]) + '\\n')
count = len(log.read_text().splitlines())
if os.environ.get('BLOCK') == 'true':
    def stop(*unused):
        pathlib.Path(os.environ['STOP_LOG']).write_text('TERM')
        sys.exit(143)
    signal.signal(signal.SIGTERM, stop)
    pathlib.Path(os.environ['READY']).write_text('ready')
    while True:
        time.sleep(.05)
sys.exit(75 if count < int(os.environ.get('LAUNCH_LIMIT', '4')) else int(os.environ.get('FINAL_CODE', '0')))
""")
        self.stub.chmod(0o700)
        (self.root / "bootstrap").write_text(
            f"JAVACMD={self.stub}\nCOMMON_JAVA_OPTS=''\nCLASSPATH=Main\n"
        )
        template = (REPO / "packaging/docker/docker-entrypoint.sh").read_text()
        self.entrypoint = self.root / "entrypoint.sh"
        self.entrypoint.write_text(template.replace("@LIB_DIR@", str(self.root))
                                   .replace("@BOOTSTRAP_FILE_NAME@", "bootstrap")
                                   .replace("@CONF_DIR@", str(self.root)))
        self.env = dict(os.environ, JVM_MAX_MEM="64m", JVM_MIN_MEM="64m", JVM_GC="G1GC", JVM_G1HeapRegionSize="1m",
                        LAUNCH_LOG=str(self.log), STOP_LOG=str(self.root / "stopped"), READY=str(self.root / "ready"),
                        IGNITE_STALE_RECOVERY_ENABLED="true")

    def tearDown(self):
        self.temporary.cleanup()

    def run_entrypoint(self):
        return subprocess.run(["sh", str(self.entrypoint), "--node-name", "test"], env=self.env,
                              capture_output=True, text=True, timeout=10)

    def test_only_75_relaunches_and_property_precedes_main_class(self):
        result = self.run_entrypoint()
        self.assertEqual(0, result.returncode, result.stderr)
        launches = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(4, len(launches))
        for args in launches:
            self.assertLess(args.index("-Dignite.staleRecovery.enabled=true"), args.index("Main"))
        self.assertEqual(3, result.stdout.count("STALE_RECOVERY_RELAUNCH"))

    def test_unrelated_failure_is_not_restarted(self):
        self.env.update(LAUNCH_LIMIT="1", FINAL_CODE="41")
        result = self.run_entrypoint()
        self.assertEqual(41, result.returncode, result.stderr)
        self.assertEqual(1, len(self.log.read_text().splitlines()))

    def test_disabled_supervisor_propagates_75(self):
        self.env["IGNITE_STALE_RECOVERY_ENABLED"] = "false"
        result = self.run_entrypoint()
        self.assertEqual(75, result.returncode, result.stderr)
        self.assertEqual(1, len(self.log.read_text().splitlines()))

    def test_term_is_forwarded_without_relaunch(self):
        self.env["BLOCK"] = "true"
        process = subprocess.Popen(["sh", str(self.entrypoint)], env=self.env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 5
            while not (self.root / "ready").exists() and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue((self.root / "ready").exists())
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(143, process.returncode, stderr)
            self.assertEqual("TERM", (self.root / "stopped").read_text())
            self.assertNotIn("STALE_RECOVERY_RELAUNCH", stdout)
        finally:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
