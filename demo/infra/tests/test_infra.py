"""Infrastructure checks: run on every machine before the load test.

Plain pytest. The pipeline passes parameters as environment variables and
reads two things back: junit.xml (kept as an artifact) and infra.json (the
measured numbers, printed to stdout by the job's command).
"""
import json
import os
import socket
import statistics
import time

import pytest

OUT_DIR = os.environ.get("OUT_DIR", "/work/out")
MIN_CPUS = int(os.environ.get("MIN_CPUS", "2"))
DISK_MIB = int(os.environ.get("DISK_MIB", "256"))
MIN_DISK_MBPS = float(os.environ.get("MIN_DISK_MBPS", "50"))
PEER = os.environ.get("PEER", "")  # host:port of the database
MAX_CONNECT_MS = float(os.environ.get("MAX_CONNECT_MS", "20"))

measured = {}


@pytest.fixture(scope="session", autouse=True)
def write_measurements():
    os.makedirs(OUT_DIR, exist_ok=True)
    yield
    with open(os.path.join(OUT_DIR, "infra.json"), "w") as f:
        json.dump(measured, f)


def test_cpu_count(record_property):
    cpus = os.cpu_count() or 0
    measured["cpus"] = cpus
    record_property("cpus", cpus)
    assert cpus >= MIN_CPUS, f"{cpus} cpu < {MIN_CPUS}"


def test_disk_fsync_throughput(record_property):
    """Sequential write of DISK_MIB MiB, fsync included, on the machine's disk."""
    path = os.path.join(OUT_DIR, "probe.bin")
    chunk = os.urandom(1 << 20)
    started = time.perf_counter()
    with open(path, "wb") as f:
        for _ in range(DISK_MIB):
            f.write(chunk)
        f.flush()
        os.fsync(f.fileno())
    mbps = DISK_MIB / (time.perf_counter() - started)
    os.remove(path)
    measured["diskMBps"] = round(mbps, 1)
    record_property("disk_mbps", round(mbps, 1))
    assert mbps >= MIN_DISK_MBPS, f"{mbps:.1f} MiB/s < {MIN_DISK_MBPS}"


@pytest.mark.skipif(not PEER, reason="PEER is not set")
def test_tcp_connect_latency(record_property):
    """p99 of a TCP connect to the database port: the network between the machines."""
    host, port = PEER.rsplit(":", 1)
    samples = []
    for _ in range(100):
        started = time.perf_counter()
        with socket.create_connection((host, int(port)), timeout=3):
            pass
        samples.append((time.perf_counter() - started) * 1000)
    p99 = statistics.quantiles(samples, n=100)[98]
    measured["connectP99Ms"] = round(p99, 2)
    record_property("connect_p99_ms", round(p99, 2))
    assert p99 <= MAX_CONNECT_MS, f"p99 {p99:.2f} ms > {MAX_CONNECT_MS}"
