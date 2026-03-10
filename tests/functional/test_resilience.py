# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering RabbitMQ resiliency under quorum queue load."""

from __future__ import (
    annotations,
)

import time
import uuid
from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    amqp_uri,
    deploy_local,
    loadbalancer_address,
    resilience_profile,
    run_action,
    start_perf_test,
    wait_for_app,
    wait_for_running_nodes,
)


def test_resilience_during_scale_events(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Sustain quorum queue traffic while the cluster scales down and recovers."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base, units=3)

    loadbalancer_ip = loadbalancer_address(juju, app_name)
    username = f"resilience-{uuid.uuid4().hex[:8]}"
    vhost = f"resilience-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        f"{app_name}/0",
        "get-service-account",
        {"username": username, "vhost": vhost},
    )
    uri = amqp_uri(
        username=service_account.results["username"],
        password=service_account.results["password"],
        host=loadbalancer_ip,
        vhost=service_account.results["vhost"],
    )

    profile = resilience_profile()
    perf_test = start_perf_test(
        uri=uri,
        profile=profile,
        test_id=f"resilience-{uuid.uuid4().hex[:8]}",
        queue_prefix="resilience",
    )
    try:
        time.sleep(20)

        juju.cli("scale-application", app_name, "2")
        wait_for_app(juju, app_name, units=2)
        status = juju.status()
        surviving_units = sorted(status.apps[app_name].units)
        expected_nodes = {
            f"rabbit@{unit.replace('/', '-')}.{app_name}-endpoints"
            for unit in surviving_units
        }
        wait_for_running_nodes(juju, surviving_units[0], expected_nodes)

        juju.add_unit(app_name, num_units=1)
        wait_for_app(juju, app_name, units=3)
        status = juju.status()
        current_units = sorted(status.apps[app_name].units)
        expected_nodes = {
            f"rabbit@{unit.replace('/', '-')}.{app_name}-endpoints"
            for unit in current_units
        }
        wait_for_running_nodes(juju, current_units[0], expected_nodes)

        stdout, stderr = perf_test.communicate(
            timeout=profile["duration"] + 60
        )
    finally:
        if perf_test.poll() is None:
            perf_test.terminate()
            perf_test.wait(timeout=30)

    assert perf_test.returncode == 0, (
        "rabbitmq-perf-test failed during resilience test\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )
