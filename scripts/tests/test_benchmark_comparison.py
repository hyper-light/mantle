"""Exercise the runner with real child processes, without running a performance workload."""
import json
import os
import pathlib
import subprocess
import tempfile
import unittest


RUNNER = pathlib.Path(__file__).resolve().parents[1] / "compare-engine-benchmarks.py"
FAKE = '''#!/usr/bin/env python3
import json, os, pathlib, sys
with open(os.environ["BENCH_CALL_LOG"], "a") as stream:
    stream.write(json.dumps(sys.argv) + "\\n")
if pathlib.Path(sys.argv[0]).name == "fail":
    sys.exit(3)
rocks = any(arg.startswith("--benchmarks=") for arg in sys.argv)
for workload in ["fillrandom", "readrandom", "seekrandom"]:
    if rocks:
        print(workload + " : 2.000 micros/op 500000 ops/sec 0.020 seconds 10000 operations")
        print("Min: 0 Median: 1 Max: 15")
        print("Percentiles: P50: 1 P99: 2 P99.9: 3 P99.99: 4")
    else:
        print(workload + " latency_us p50 1 p99 2 p99.9 3 p99.99 4 max 15")
        print(workload + " 10000 500000 2.000")
        print(workload + " process_cpu_ns/op 2000.000 user_ns/op 1800.000 system_ns/op 200.000 instructions/op unavailable cycles/op 3000.000 scope whole-process-including-issuer-and-device-workers")
if not rocks:
    print("workload generator rocksdb-mt19937_64 seed Some(301)")
    print("maintain 0.125000s")
    print("maintain process_cpu_ns/op 12.500 user_ns/op 10.000 system_ns/op 2.500 instructions/op unavailable cycles/op unavailable scope whole-process-including-issuer-and-device-workers")
    print("filter budget 30000 bytes 24.00 bits/input-key")
    print("workers owner 1 issuer 1 device 16 total 18")
'''


class Comparison(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mantle-runner-test-")
        self.directory = pathlib.Path(self.temporary.name)
        self.source = self.directory / "source"
        self.source.mkdir()
        subprocess.run(["git", "init", "-q", str(self.source)], check=True)
        subprocess.run(["git", "-C", str(self.source), "-c", "user.name=Test",
                        "-c", "user.email=test@example.invalid", "commit", "-q",
                        "--allow-empty", "-m", "fixture"], check=True)
        self.binary = self.directory / "fake"
        self.binary.write_text(FAKE)
        self.binary.chmod(0o755)
        self.calls = self.directory / "calls.jsonl"
        self.environment = dict(os.environ, BENCH_CALL_LOG=str(self.calls))

    def tearDown(self):
        self.temporary.cleanup()

    def command(self, output):
        return ["python3", str(RUNNER), "--mantle-binary", f"before={self.binary}",
                "--mantle-source", f"before={self.source}", "--output", str(output),
                "--num", "10000", "--reads", "1000"]

    def test_alternates_and_preserves_paired_flags_tails_and_provenance(self):
        output = self.directory / "comparison"
        command = self.command(output) + [
            "--mantle-binary", f"after={self.binary}",
            "--mantle-source", f"after={self.source}",
            "--rocksdb-binary", str(self.binary), "--rocksdb-source", str(self.source),
            "--rounds", "2"]
        subprocess.run(command, env=self.environment, check=True, capture_output=True)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertEqual([pathlib.Path(call[0]).name for call in calls],
                         ["before", "after", "rocksdb", "rocksdb", "after", "before"])
        for first, second in [(calls[0], calls[1]), (calls[4], calls[5])]:
            self.assertEqual(first[2:], second[2:])
        self.assertIn("--rocks-seed=301", calls[0])
        self.assertIn("--seed=301", calls[2])
        self.assertIn("--rocks-seed=302", calls[5])
        self.assertIn("--bloom_bits=24", calls[2])
        self.assertIn("--benchmarks=fillrandom,waitforcompaction,readrandom,seekrandom", calls[2])
        results = json.loads((output / "results.json").read_text())
        self.assertEqual(len(results), 6)
        for result in results:
            for metric in result["metrics"].values():
                self.assertEqual(metric["latency_us"],
                                 {"p50": 1, "p99": 2, "p99.9": 3, "p99.99": 4, "max": 15})
            self.assertIn("process_peak_rss_bytes", result)
            self.assertIn("process_user_seconds", result)
            self.assertTrue((output / f"round-{result['round']}-{result['label']}" / "resources.csv").exists())
        metadata = json.loads((output / "metadata.json").read_text())
        self.assertEqual(metadata["binaries"]["before"]["sha256"],
                         metadata["binaries"]["after"]["sha256"])
        self.assertEqual(metadata["sources"]["before"]["status"]["stdout"], "")
        self.assertTrue(metadata["sources"]["before"]["dirty_content_sha256"])
        self.assertTrue(any(line.startswith("workload ")
                            for line in results[0]["memory_and_filters"]))
        self.assertTrue(any(line.startswith("workers ")
                            for line in results[0]["memory_and_filters"]))
        self.assertTrue(any(line.startswith("filter budget ")
                            for line in results[0]["memory_and_filters"]))
        phase = results[0]["phase_process_accounting"]["fillrandom"]
        self.assertEqual(phase["cpu_ns_per_op"], 2000.0)
        self.assertIsNone(phase["instructions_per_op"])
        self.assertEqual(phase["cycles_per_op"], 3000.0)
        self.assertEqual(results[0]["phase_process_accounting"]["maintain"]["cpu_ns_per_op"], 12.5)
        self.assertEqual(len(results[0]["phase_process_accounting_lines"]), 4)

    def test_single_configuration_and_failed_process_preserve_evidence(self):
        output = self.directory / "single"
        subprocess.run(self.command(output) + ["--single-configuration", "--rounds", "1",
                                              "--maintenance", "deferred"],
                       env=self.environment, check=True, capture_output=True)
        call = json.loads(self.calls.read_text().splitlines()[0])
        self.assertIn("--maintenance=deferred", call)
        failed = self.directory / "failed"
        command = ["fail=" + arg.partition("=")[2] if arg.startswith("before=") else arg
                   for arg in self.command(failed)]
        result = subprocess.run(command + ["--single-configuration", "--rounds", "1"],
                                env=self.environment, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        record = json.loads((failed / "round-1-fail/run.json").read_text())
        self.assertEqual(record["exit_code"], 3)
        self.assertTrue((failed / "round-1-fail/raw.txt").exists())
        self.assertFalse((failed / "results.json").exists())


if __name__ == "__main__":
    unittest.main()
