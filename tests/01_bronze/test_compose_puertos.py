"""
The administration ports of the stack are published on 127.0.0.1 only, so
nobody on the same network (the one of the university during the defence)
reaches the consoles of MinIO, Spark, Trino or Kafka UI. The test reads
compose.yaml, without Docker; `docker compose config` gives the same
host_ip on the laptop. It also checks that Trino keeps its memory cap.

    python -m pytest tests/01_bronze/test_compose_puertos.py -q
"""

import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Container ports of administration, by service.
LOCAL_ONLY = {
    "minio": {"9001"},
    "spark-master": {"8080", "4040"},
    "spark-worker": {"8081"},
    "spark-history": {"18080"},
    "trino": {"8080"},
    "kafka-ui": {"8080"},
}


def published_ports() -> dict:
    """{service: [port line, ...]} of compose.yaml."""
    with open(os.path.join(ROOT, "compose.yaml"), encoding="utf-8") as file:
        lines = file.read().splitlines()
    out, service, in_ports = {}, None, False
    for line in lines:
        top = re.match(r"^  ([a-z0-9_-]+):\s*$", line)
        if top:
            service, in_ports = top.group(1), False
            continue
        if re.match(r"^    ports:\s*$", line):
            in_ports = True
            continue
        if in_ports:
            item = re.match(r'^\s+- "([^"]+)"', line)
            if item:
                out.setdefault(service, []).append(item.group(1))
            elif line.strip() and not line.strip().startswith("#"):
                in_ports = False
    return out


def test_the_administration_ports_only_listen_on_the_laptop():
    ports = published_ports()
    for service, targets in LOCAL_ONLY.items():
        assert service in ports, f"{service} has no ports in compose.yaml"
        for entry in ports[service]:
            container = entry.rsplit(":", 1)[-1]
            if container in targets:
                assert entry.startswith("127.0.0.1:"), f"{service} publishes {entry} on every interface"
    local = [e for s in LOCAL_ONLY for e in ports[s] if e.startswith("127.0.0.1:")]
    assert len(local) == 7


def service_keys(service: str) -> dict:
    """{key: value} of the second-level keys of one service of compose.yaml."""
    with open(os.path.join(ROOT, "compose.yaml"), encoding="utf-8") as file:
        lines = file.read().splitlines()
    out, inside = {}, False
    for line in lines:
        top = re.match(r"^  ([a-z0-9_-]+):\s*$", line)
        if top:
            inside = top.group(1) == service
            continue
        key = re.match(r"^    ([a-z_]+):\s*(.*)$", line)
        if inside and key:
            out[key.group(1)] = key.group(2).strip()
    return out


def test_trino_has_a_memory_cap():
    # Without it the JVM of the image takes 80 % of the Docker virtual machine (18.8 GiB on the laptop).
    assert service_keys("trino").get("mem_limit") == "4g"
