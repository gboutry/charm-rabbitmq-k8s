# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for Jubilant-based functional tests."""

from __future__ import (
    annotations,
)

import json
import os
import shutil
import subprocess  # nosec B404
import time
from pathlib import (
    Path,
)
from urllib.parse import (
    quote,
)

import jubilant
import pytest

RABBITMQ_CONTAINER = "rabbitmq"


def wait_for_app(
    juju: jubilant.Juju, app_name: str, units: int
) -> jubilant.Status:
    """Wait until the application is active, idle, and at the expected scale."""

    def is_ready(status: jubilant.Status) -> bool:
        app = status.apps.get(app_name)
        if app is None:
            return False
        return (
            len(app.units) == units
            and jubilant.all_active(status, app_name)
            and jubilant.all_agents_idle(status, app_name)
        )

    return juju.wait(is_ready, timeout=20 * 60)


def deploy_local(
    juju: jubilant.Juju,
    app_name: str,
    charm_file: Path,
    rabbitmq_image: str,
    base: str,
    units: int = 1,
) -> jubilant.Status:
    """Deploy the local charm artifact and wait for it to settle."""
    juju.deploy(
        str(charm_file),
        app=app_name,
        base=base,
        num_units=units,
        resources={"rabbitmq-image": rabbitmq_image},
        trust=True,
    )
    return wait_for_app(juju, app_name, units)


def deploy_stable(
    juju: jubilant.Juju,
    app_name: str,
    stable_channel: str,
    base: str,
    units: int = 1,
) -> jubilant.Status:
    """Deploy the charm from Charmhub stable and wait for it to settle."""
    juju.deploy(
        app_name,
        app=app_name,
        base=base,
        channel=stable_channel,
        num_units=units,
        trust=True,
    )
    return wait_for_app(juju, app_name, units)


def run_action(
    juju: jubilant.Juju,
    unit_name: str,
    action_name: str,
    params: dict | None = None,
) -> jubilant.Task:
    """Run an action and assert that it completed successfully."""
    task = juju.run(unit_name, action_name, params or {})
    assert task.success, task
    return task


def expected_rabbit_node(app_name: str, unit_name: str) -> str:
    """Return the RabbitMQ node name for a Juju unit."""
    return f"rabbit@{unit_name.replace('/', '-')}.{app_name}-endpoints"


def cluster_status(juju: jubilant.Juju, unit_name: str) -> dict:
    """Return parsed cluster status from inside the workload container."""
    output = juju.ssh(
        unit_name,
        "rabbitmqctl",
        "cluster_status",
        "--formatter=json",
        container=RABBITMQ_CONTAINER,
    )
    return json.loads(output)


def wait_for_running_nodes(
    juju: jubilant.Juju,
    unit_name: str,
    expected_nodes: set[str],
    timeout: int = 180,
    interval: int = 5,
) -> dict:
    """Wait until RabbitMQ reports the expected running nodes."""
    deadline = time.monotonic() + timeout
    last_status = {}
    while time.monotonic() < deadline:
        last_status = cluster_status(juju, unit_name)
        if set(last_status["running_nodes"]) == expected_nodes:
            return last_status
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for running nodes {expected_nodes}; "
        f"last status was {last_status}"
    )


def model_name(juju: jubilant.Juju) -> str:
    """Return the active Juju model name."""
    return juju.show_model().name.split("/")[-1]


def loadbalancer_address(
    juju: jubilant.Juju,
    app_name: str,
    timeout: int = 300,
    interval: int = 5,
) -> str:
    """Return the external address of the RabbitMQ load balancer service."""
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        pytest.skip("kubectl is required for resiliency functional tests")

    namespace = model_name(juju)
    service_name = f"{app_name}-lb"
    jsonpath = "{.status.loadBalancer.ingress[0].ip}{.status.loadBalancer.ingress[0].hostname}"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        output = subprocess.run(
            [
                kubectl,
                "get",
                "service",
                service_name,
                "-n",
                namespace,
                "-o",
                f"jsonpath={jsonpath}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )  # nosec B603
        address = output.stdout.strip()
        if address:
            return address
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for LoadBalancer address for {service_name}"
    )


def resilience_profile() -> dict[str, int]:
    """Build a conservative perf-test profile based on local CPU capacity."""
    cpus = max(2, os.cpu_count() or 2)
    return {
        "queues": min(48, max(8, cpus * 2)),
        "producers": min(16, max(4, cpus // 2)),
        "consumers": min(16, max(4, cpus // 2)),
        "rate": min(160, max(40, cpus * 4)),
        "confirm": 10,
        "qos": 10,
        "size": 1024,
        "duration": 120,
        "start_delay": min(30, max(10, cpus)),
        "heartbeat_threads": min(4, max(1, cpus // 8)),
        "consumer_pools": min(8, max(2, cpus // 3)),
        "nio_threads": min(8, max(2, cpus // 4)),
    }


def start_perf_test(
    *,
    uri: str,
    profile: dict[str, int],
    test_id: str,
    queue_prefix: str,
) -> subprocess.Popen[str]:
    """Start a bounded rabbitmq-perf-test process."""
    perf_test = shutil.which("rabbitmq-perf-test")
    if perf_test is None:
        pytest.skip(
            "rabbitmq-perf-test is required for resiliency functional tests"
        )

    args = [
        perf_test,
        "--uri",
        uri,
        "--id",
        test_id,
        "--queue-pattern",
        f"{queue_prefix}-%03d",
        "--queue-pattern-from",
        "1",
        "--queue-pattern-to",
        str(profile["queues"]),
        "--producers",
        str(profile["producers"]),
        "--consumers",
        str(profile["consumers"]),
        "--quorum-queue",
        "--leader-locator",
        "balanced",
        "--confirm",
        str(profile["confirm"]),
        "--qos",
        str(profile["qos"]),
        "--multi-ack-every",
        "5",
        "--rate",
        str(profile["rate"]),
        "--size",
        str(profile["size"]),
        "--producer-random-start-delay",
        str(profile["start_delay"]),
        "--heartbeat-sender-threads",
        str(profile["heartbeat_threads"]),
        "--consumers-thread-pools",
        str(profile["consumer_pools"]),
        "--nio-threads",
        str(profile["nio_threads"]),
        "--time",
        str(profile["duration"]),
    ]
    return subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )  # nosec B603


def amqp_uri(
    *,
    username: str,
    password: str,
    host: str,
    vhost: str,
    port: int = 5672,
) -> str:
    """Build an AMQP URI for rabbitmq-perf-test."""
    return (
        f"amqp://{username}:{password}@{host}:{port}/{quote(vhost, safe='')}"
    )
