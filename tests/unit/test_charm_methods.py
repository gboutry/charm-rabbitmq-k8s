# Copyright 2026 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Mock-driven unit tests for RabbitMQOperatorCharm helpers."""

from pathlib import (
    Path,
)
from types import (
    SimpleNamespace,
)
from unittest.mock import (
    MagicMock,
    Mock,
    call,
    patch,
)

import httpx
import ops.model
import ops.pebble
import pytest
import requests

import charm


def test_grafana_dashboard_uses_cos_datasource_convention():
    """Bundled dashboards should not ship legacy Grafana datasource placeholders."""
    for dashboard_path in Path("src/grafana_dashboards").glob("*.json"):
        dashboard = dashboard_path.read_text()

        assert "${DS_PROMETHEUS}" not in dashboard
        assert '"__inputs"' not in dashboard
        assert "${prometheusds}" in dashboard


def _fake_charm(**kwargs):
    """Build a thin object that can exercise charm helper methods."""
    base = {
        "app": SimpleNamespace(name="rabbitmq-k8s"),
        "unit": Mock(),
        "peers": SimpleNamespace(
            erlang_cookie=None,
            set_erlang_cookie=Mock(),
            operator_user_created=None,
        ),
        "_get_admin_api": Mock(),
        "min_replicas": lambda: 3,
        "peers_bind_address": "10.10.1.1",
        "get_hostname": Mock(return_value="rabbitmq-k8s-endpoints"),
        "does_vhost_exist": Mock(return_value=True),
        "create_vhost": Mock(),
        "does_user_exist": Mock(return_value=True),
        "create_user": Mock(return_value="new-password"),
        "set_user_permissions": Mock(),
        "_on_update_status": Mock(),
        "generate_nodename": lambda unit_name: charm.RabbitMQOperatorCharm.generate_nodename(
            SimpleNamespace(app=SimpleNamespace(name="rabbitmq-k8s")),
            unit_name,
        ),
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


class FakeConnectionError(requests.exceptions.ConnectionError):
    """Connection error carrying an errno for charm logging paths."""

    def __init__(self, message: str, errno: int = 111):
        super().__init__(message)
        self.errno = errno


def test_get_queue_growth_selector():
    """The queue growth selector follows the existing sizing rules."""
    fake = _fake_charm()

    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 1, 1)
        == "all"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 1, 2)
        == "all"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 1, 3)
        == "individual"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 2, 2)
        == "all"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 2, 3)
        == "even"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 3, 3)
        == "even"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 3, 5)
        == "even"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 4, 4)
        == "even"
    )


def test_generate_nodename():
    """Unit names are converted to the k8s RabbitMQ node format."""
    fake = _fake_charm()

    assert (
        charm.RabbitMQOperatorCharm.generate_nodename(fake, "unit/1")
        == "rabbit@unit-1.rabbitmq-k8s-endpoints"
    )


def test_unit_in_cluster():
    """Cluster membership is determined from the admin API node list."""
    admin_api = Mock()
    admin_api.list_nodes.return_value = [
        {"name": "rabbit@unit-1.rabbitmq-k8s-endpoints"}
    ]
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    assert charm.RabbitMQOperatorCharm.unit_in_cluster(fake, "unit/1")
    assert not charm.RabbitMQOperatorCharm.unit_in_cluster(fake, "unit/2")


def test_grow_queues_onto_unit():
    """Growing queues chooses the correct API call for each selector."""
    queue_one_member = {
        "name": "queue1",
        "vhost": "openstack",
        "members": ["node1"],
    }
    queue_two_member = {
        "name": "queue2",
        "vhost": "openstack",
        "members": ["node1", "node2"],
    }
    queue_three_member = {
        "name": "queue3",
        "vhost": "openstack",
        "members": ["node1", "node2", "node3"],
    }
    admin_api = Mock()
    fake = _fake_charm(
        _get_admin_api=Mock(return_value=admin_api),
        get_queue_growth_selector=Mock(
            side_effect=["all", "even", "individual"]
        ),
        get_undersized_queues=Mock(
            return_value=[
                queue_one_member,
                queue_two_member,
                queue_three_member,
            ]
        ),
    )

    admin_api.list_quorum_queues.return_value = [queue_one_member]
    charm.RabbitMQOperatorCharm.grow_queues_onto_unit(fake, "unit/1")
    admin_api.grow_queue.assert_called_once_with(
        "rabbit@unit-1.rabbitmq-k8s-endpoints", "all"
    )

    admin_api.grow_queue.reset_mock()
    admin_api.list_quorum_queues.return_value = [
        queue_two_member,
        queue_three_member,
    ]
    charm.RabbitMQOperatorCharm.grow_queues_onto_unit(fake, "unit/1")
    admin_api.grow_queue.assert_called_once_with(
        "rabbit@unit-1.rabbitmq-k8s-endpoints", "even"
    )

    admin_api.grow_queue.reset_mock()
    admin_api.list_quorum_queues.return_value = [
        queue_one_member,
        queue_two_member,
        queue_three_member,
    ]
    charm.RabbitMQOperatorCharm.grow_queues_onto_unit(fake, "unit/1")
    admin_api.add_member.assert_has_calls(
        [
            call(
                "rabbit@unit-1.rabbitmq-k8s-endpoints",
                "openstack",
                "queue1",
            ),
            call(
                "rabbit@unit-1.rabbitmq-k8s-endpoints",
                "openstack",
                "queue2",
            ),
        ]
    )


def test_no_undersized_queues():
    """No queue changes are made when nothing is undersized."""
    admin_api = Mock()
    admin_api.list_quorum_queues.return_value = []

    result = charm.RabbitMQOperatorCharm._add_members_to_undersized_queues(
        SimpleNamespace(), admin_api, ["node1", "node2", "node3"], [], 3, False
    )

    assert result == []


def test_not_enough_nodes_to_replicate():
    """Queues are still replicated onto available nodes when possible."""
    queues = [{"name": "queue1", "members": ["node1"], "vhost": "/"}]
    admin_api = Mock()
    admin_api.list_quorum_queues.return_value = queues

    result = charm.RabbitMQOperatorCharm._add_members_to_undersized_queues(
        SimpleNamespace(),
        admin_api,
        ["node1", "node2"],
        queues,
        3,
        False,
    )

    assert result == ["queue1"]
    admin_api.add_member.assert_called_once_with("node2", "/", "queue1")


def test_exact_number_of_nodes_needed():
    """Queues are added to the nodes with the lowest current membership."""
    admin_api = Mock()
    admin_api.list_quorum_queues.return_value = [
        {"name": "queue1", "members": ["node1"], "vhost": "/"},
        {"name": "queue2", "members": ["node2"], "vhost": "/"},
        {
            "name": "queue3",
            "members": ["node1", "node2", "node3"],
            "vhost": "/",
        },
    ]
    undersized_queues = [
        {"name": "queue1", "members": ["node1"], "vhost": "/"},
        {"name": "queue2", "members": ["node2"], "vhost": "/"},
    ]

    result = charm.RabbitMQOperatorCharm._add_members_to_undersized_queues(
        SimpleNamespace(),
        admin_api,
        ["node1", "node2", "node3"],
        undersized_queues,
        3,
        False,
    )

    assert result == ["queue1", "queue2"]
    admin_api.add_member.assert_has_calls(
        [
            call("node3", "/", "queue1"),
            call("node2", "/", "queue1"),
            call("node3", "/", "queue2"),
            call("node1", "/", "queue2"),
        ]
    )


@pytest.mark.parametrize(
    ("annotations", "expected_result"),
    [
        ("key1=value1,key2=value2", {"key1": "value1", "key2": "value2"}),
        ("", {}),
        (
            "key1=value1,key_2=value2,key-3=value3,",
            {"key1": "value1", "key_2": "value2", "key-3": "value3"},
        ),
        (
            "key1=value1,key2=value2,key3=value3",
            {"key1": "value1", "key2": "value2", "key3": "value3"},
        ),
        ("example.com/key=value", {"example.com/key": "value"}),
        (
            "prefix1/key=value1,prefix2/another-key=value2",
            {"prefix1/key": "value1", "prefix2/another-key": "value2"},
        ),
        (
            "key=value,key.sub-key=value-with-hyphen",
            {"key": "value", "key.sub-key": "value-with-hyphen"},
        ),
        ("key1=value1,key2=value2,key=value3,key4=", None),
        ("kubernetes.io/description=this-is-valid,custom.io/key=value", None),
        ("key1=value1,key2", None),
        ("key1=value1,example..com/key2=value2", None),
        ("key1=value1,key=value2,key3=", None),
        ("key1=value1,=value2", None),
        ("key1=value1,key=val=ue2", None),
        ("a" * 256 + "=value", None),
        ("key@=value", None),
        ("key. =value", None),
        ("key,value", None),
        ("kubernetes/description=", None),
    ],
)
def test_lb_annotations(annotations, expected_result):
    """Load-balancer annotations are parsed and validated."""
    assert charm.parse_annotations(annotations) == expected_result


def test_ensure_ownership_same_ownership():
    """Ownership is unchanged when it already matches the desired values."""
    container = Mock()
    file_info = Mock(user="rabbitmq", group="rabbitmq")
    container.list_files.return_value = [file_info]

    result = charm.RabbitMQOperatorCharm._ensure_ownership(
        SimpleNamespace(), container, "/test/path", "rabbitmq", "rabbitmq"
    )

    assert result is True
    container.list_files.assert_called_once_with("/test/path", itself=True)
    container.exec.assert_not_called()


def test_ensure_ownership_different_ownership():
    """Ownership is corrected when the target path is owned by root."""
    container = Mock()
    file_info = Mock(user="root", group="root")
    container.list_files.return_value = [file_info]

    result = charm.RabbitMQOperatorCharm._ensure_ownership(
        SimpleNamespace(), container, "/test/path", "rabbitmq", "rabbitmq"
    )

    assert result is True
    container.exec.assert_called_once_with(
        ["chown", "-R", "rabbitmq:rabbitmq", "/test/path"]
    )


def test_ensure_ownership_exec_error():
    """Ownership changes fail gracefully when chown raises ExecError."""
    container = Mock()
    file_info = Mock(user="root", group="root")
    container.list_files.return_value = [file_info]
    container.exec.side_effect = ops.pebble.ExecError(
        ["chown"], 1, "stdout", "Permission denied"
    )

    result = charm.RabbitMQOperatorCharm._ensure_ownership(
        SimpleNamespace(), container, "/test/path", "rabbitmq", "rabbitmq"
    )

    assert result is False


def test_set_ownership_on_data_dir_not_mounted():
    """Data-dir ownership waits until the storage mount exists."""
    container = Mock()
    container.exec.side_effect = ops.pebble.ExecError(
        ["mountpoint"], 1, "", "not a mountpoint"
    )
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is False
    container.exec.assert_called_once_with(
        ["mountpoint", charm.RABBITMQ_DATA_DIR]
    )


def test_set_ownership_on_data_dir_ensure_ownership_fails():
    """Data-dir ownership stops on the first ownership failure."""
    container = Mock()
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
    fake._ensure_ownership = Mock(return_value=False)

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is False
    fake._ensure_ownership.assert_called_once_with(
        container,
        charm.RABBITMQ_DATA_DIR,
        charm.RABBITMQ_USER,
        charm.RABBITMQ_GROUP,
    )


def test_set_ownership_on_data_dir_no_mnesia_dir():
    """The mnesia directory is created when it does not exist."""
    container = Mock()
    container.list_files.return_value = []
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
    fake._ensure_ownership = Mock(return_value=True)

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is True
    container.make_dir.assert_called_once_with(
        charm.RABBITMQ_MNESIA_DIR,
        permissions=0o750,
        user=charm.RABBITMQ_USER,
        group=charm.RABBITMQ_GROUP,
    )


def test_set_ownership_on_data_dir_mnesia_exists():
    """Both data and mnesia ownership are enforced when mnesia exists."""
    container = Mock()
    container.list_files.return_value = [Mock()]
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
    fake._ensure_ownership = Mock(return_value=True)

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is True
    fake._ensure_ownership.assert_has_calls(
        [
            call(
                container,
                charm.RABBITMQ_DATA_DIR,
                charm.RABBITMQ_USER,
                charm.RABBITMQ_GROUP,
            ),
            call(
                container,
                charm.RABBITMQ_MNESIA_DIR,
                charm.RABBITMQ_USER,
                charm.RABBITMQ_GROUP,
            ),
        ]
    )


def test_set_ownership_on_data_dir_mnesia_ownership_fails():
    """A mnesia ownership failure returns False."""
    container = Mock()
    container.list_files.return_value = [Mock()]
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
    fake._ensure_ownership = Mock(side_effect=[True, False])

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is False


def test_ensure_erlang_cookie_with_existing_peer_cookie():
    """An existing peer cookie is written into the workload container."""
    container = Mock()
    peers = SimpleNamespace(
        erlang_cookie="existing-cookie-value",
        set_erlang_cookie=Mock(),
    )
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        peers=peers,
    )

    charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_called_once_with(
        charm.RABBITMQ_COOKIE_PATH,
        "existing-cookie-value",
        permissions=0o600,
        make_dirs=True,
        user=charm.RABBITMQ_USER,
        group=charm.RABBITMQ_GROUP,
    )


def test_ensure_erlang_cookie_generate_new_when_leader():
    """The leader generates and stores a cookie when none exists yet."""
    container = Mock()
    container.exists.return_value = False
    peers = SimpleNamespace(
        erlang_cookie="",
        set_erlang_cookie=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = True
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit, peers=peers)

    with patch(
        "charm.secrets.token_hex", return_value="generated-cookie-value"
    ):
        charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_called_once_with(
        path=charm.RABBITMQ_COOKIE_PATH,
        source="generated-cookie-value",
        make_dirs=True,
        user=charm.RABBITMQ_USER,
        group=charm.RABBITMQ_GROUP,
        permissions=0o600,
    )
    peers.set_erlang_cookie.assert_called_once_with("generated-cookie-value")


def test_ensure_erlang_cookie_no_action_when_not_leader():
    """Non-leaders do not generate a new cookie."""
    container = Mock()
    container.exists.return_value = False
    peers = SimpleNamespace(
        erlang_cookie=None,
        set_erlang_cookie=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = False
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit, peers=peers)

    charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_not_called()
    peers.set_erlang_cookie.assert_not_called()


def test_ensure_erlang_cookie_no_action_when_file_exists():
    """No new cookie is written when the file already exists."""
    container = Mock()
    container.exists.return_value = True
    peers = SimpleNamespace(
        erlang_cookie="",
        set_erlang_cookie=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = True
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit, peers=peers)

    charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_not_called()
    peers.set_erlang_cookie.assert_not_called()


def test_publish_relation_data_on_leader():
    """Leaders publish the hostname to active AMQP relations with credentials."""
    app = object()
    databag = {"password": "the_password"}
    relation = SimpleNamespace(
        active=True,
        data={app: databag},
        name="amqp",
        id=1,
    )
    amqp_provider = SimpleNamespace(
        external_connectivity=Mock(return_value=False)
    )
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        app=app,
        model=SimpleNamespace(
            relations={charm.AMQP_RELATION: [relation]}, unit=unit
        ),
        amqp_provider=amqp_provider,
        get_hostname=Mock(return_value="rabbitmq-k8s-endpoints"),
    )

    charm.RabbitMQOperatorCharm._publish_relation_data(fake)

    assert databag == {
        "password": "the_password",
        "hostname": "rabbitmq-k8s-endpoints",
    }


def test_publish_relation_data_on_non_leader():
    """Non-leaders do not publish AMQP relation data."""
    app = object()
    databag = {"password": "the_password"}
    relation = SimpleNamespace(
        active=True,
        data={app: databag},
        name="amqp",
        id=1,
    )
    unit = Mock()
    unit.is_leader.return_value = False
    fake = _fake_charm(
        app=app,
        model=SimpleNamespace(
            relations={charm.AMQP_RELATION: [relation]}, unit=unit
        ),
        amqp_provider=SimpleNamespace(external_connectivity=Mock()),
        get_hostname=Mock(),
    )

    charm.RabbitMQOperatorCharm._publish_relation_data(fake)

    fake.get_hostname.assert_not_called()
    assert databag == {"password": "the_password"}


def test_publish_relation_data_on_model_error():
    """Model errors while reading relation data are ignored."""
    app = object()
    databag = Mock()
    databag.get.side_effect = ops.model.ModelError(
        "ERROR permission denied (unauthorized access)"
    )
    relation = SimpleNamespace(
        active=True,
        data={app: databag},
        name="amqp",
        id=1,
    )
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        app=app,
        model=SimpleNamespace(
            relations={charm.AMQP_RELATION: [relation]}, unit=unit
        ),
        amqp_provider=SimpleNamespace(external_connectivity=Mock()),
        get_hostname=Mock(),
    )

    charm.RabbitMQOperatorCharm._publish_relation_data(fake)

    fake.get_hostname.assert_not_called()


def test_on_peer_relation_connected_defers_when_rabbit_not_running():
    """Peer relation setup waits until RabbitMQ is actually running."""
    event = Mock(defer=Mock())
    fake = _fake_charm(_rabbitmq_running=Mock(return_value=False))

    charm.RabbitMQOperatorCharm._on_peer_relation_connected(fake, event)

    event.defer.assert_called_once_with()


def test_on_peer_relation_connected_sets_cookie_and_initializes_user():
    """Leaders publish the cookie and initialize the operator user."""
    event = Mock(defer=Mock())
    container = Mock()
    cookie_file = Mock(read=Mock(return_value="magiccookie\n"))
    container.pull.return_value = MagicMock()
    container.pull.return_value.__enter__.return_value = cookie_file
    container.pull.return_value.__exit__.return_value = None
    peers = SimpleNamespace(
        erlang_cookie=None,
        operator_user_created=None,
        set_erlang_cookie=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = True
    unit.get_container.return_value = container
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=True),
        peers=peers,
        unit=unit,
        _initialize_operator_user=Mock(),
        _on_update_status=Mock(),
    )

    charm.RabbitMQOperatorCharm._on_peer_relation_connected(fake, event)

    peers.set_erlang_cookie.assert_called_once_with("magiccookie")
    fake._initialize_operator_user.assert_called_once_with()
    fake._on_update_status.assert_called_once_with(event)


def test_on_peer_relation_ready_defers_until_unit_in_cluster():
    """Peer-ready waits until the joining unit is visible in the cluster."""
    event = SimpleNamespace(nodename="rabbitmq-k8s/1", defer=Mock())
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=True),
        peers=SimpleNamespace(operator_user_created="rmqadmin"),
        unit_in_cluster=Mock(return_value=False),
    )

    charm.RabbitMQOperatorCharm._on_peer_relation_ready(fake, event)

    event.defer.assert_called_once_with()


def test_on_peer_relation_ready_leader_rebalances_when_ready():
    """Leaders grow queues and rebalance once a peer is ready."""
    event = SimpleNamespace(nodename="rabbitmq-k8s/1", defer=Mock())
    admin_api = Mock()
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=True),
        peers=SimpleNamespace(operator_user_created="rmqadmin"),
        unit=unit,
        unit_in_cluster=Mock(return_value=True),
        grow_queues_onto_unit=Mock(),
        _get_admin_api=Mock(return_value=admin_api),
        _on_update_status=Mock(),
    )

    charm.RabbitMQOperatorCharm._on_peer_relation_ready(fake, event)

    fake.grow_queues_onto_unit.assert_called_once_with("rabbitmq-k8s/1")
    admin_api.rebalance_queues.assert_called_once_with()
    fake._on_update_status.assert_called_once_with(event)


def test_on_gone_away_amqp_clients_non_leader_noop():
    """Non-leaders do not delete AMQP users on relation removal."""
    relation = Mock()
    event = SimpleNamespace(relation=relation)
    unit = Mock()
    unit.is_leader.return_value = False
    fake = _fake_charm(unit=unit)

    charm.RabbitMQOperatorCharm._on_gone_away_amqp_clients(fake, event)

    fake._get_admin_api.assert_not_called()


def test_on_gone_away_amqp_clients_leader_deletes_user():
    """Leaders clean up the user and peer password on AMQP relation removal."""
    relation = Mock()
    event = SimpleNamespace(relation=relation)
    admin_api = Mock()
    peers = SimpleNamespace(delete_user=Mock())
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        unit=unit,
        _get_admin_api=Mock(return_value=admin_api),
        amqp_provider=SimpleNamespace(username=Mock(return_value="svc-user")),
        does_user_exist=Mock(return_value=True),
        peers=peers,
    )

    charm.RabbitMQOperatorCharm._on_gone_away_amqp_clients(fake, event)

    admin_api.delete_user.assert_called_once_with("svc-user")
    peers.delete_user.assert_called_once_with("svc-user")


def test_does_user_exist_false_on_404():
    """User existence returns False when the API reports a 404."""
    admin_api = Mock()
    error = requests.exceptions.HTTPError("missing user")
    error.response = SimpleNamespace(status_code=404)
    admin_api.get_user.side_effect = error
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    assert not charm.RabbitMQOperatorCharm.does_user_exist(fake, "svc-user")


def test_does_vhost_exist_false_on_404():
    """Vhost existence returns False when the API reports a 404."""
    admin_api = Mock()
    error = requests.exceptions.HTTPError("missing vhost")
    error.response = SimpleNamespace(status_code=404)
    admin_api.get_vhost.side_effect = error
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    assert not charm.RabbitMQOperatorCharm.does_vhost_exist(fake, "/")


def test_create_user_returns_generated_password():
    """User creation returns the generated password."""
    admin_api = Mock()
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    with patch("charm.pwgen.pwgen", return_value="generated-password"):
        password = charm.RabbitMQOperatorCharm.create_user(fake, "svc-user")

    assert password == "generated-password"
    admin_api.create_user.assert_called_once_with(
        "svc-user", "generated-password"
    )


def test_set_user_permissions_calls_api():
    """Permission updates are delegated to the admin API."""
    admin_api = Mock()
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    charm.RabbitMQOperatorCharm.set_user_permissions(fake, "svc-user", "/")

    admin_api.create_user_permission.assert_called_once_with(
        "svc-user", "/", configure=".*", write=".*", read=".*"
    )


def test_create_vhost_calls_api():
    """Vhost creation is delegated to the admin API."""
    admin_api = Mock()
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    charm.RabbitMQOperatorCharm.create_vhost(fake, "/svc")

    admin_api.create_vhost.assert_called_once_with("/svc")


def test_operator_password_sets_password_on_leader():
    """The leader generates and stores the operator password when missing."""
    peers = SimpleNamespace(
        operator_password=None,
        set_operator_password=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(unit=unit, peers=peers)

    with patch("charm.pwgen.pwgen", return_value="generated-password"):
        password = charm.RabbitMQOperatorCharm._operator_password.fget(fake)

    assert password is None
    peers.set_operator_password.assert_called_once_with("generated-password")


def test_rabbit_running_uses_guest_before_operator_and_caches_version():
    """Uses the guest fallback until the operator user exists."""
    admin_api = Mock()
    admin_api.overview.return_value = {"product_version": "3.12.1"}
    fake = _fake_charm(
        peers=SimpleNamespace(operator_user_created=None),
        _get_admin_api=Mock(return_value=admin_api),
        _stored=SimpleNamespace(rabbitmq_version=None),
    )

    assert charm.RabbitMQOperatorCharm.rabbit_running.fget(fake)
    fake._get_admin_api.assert_called_once_with("guest", "guest")
    assert fake._stored.rabbitmq_version == "3.12.1"


def test_rabbit_running_returns_false_on_connection_error():
    """Connection failures report RabbitMQ as down."""
    fake = _fake_charm(
        peers=SimpleNamespace(operator_user_created="rmqadmin"),
        _get_admin_api=Mock(side_effect=FakeConnectionError("boom")),
        _stored=SimpleNamespace(rabbitmq_version=None),
    )

    assert not charm.RabbitMQOperatorCharm.rabbit_running.fget(fake)


def test_initialize_operator_user_creates_and_removes_guest():
    """The operator bootstrap creates the admin user and removes guest."""
    admin_api = Mock()
    peers = SimpleNamespace(set_operator_user_created=Mock())
    fake = _fake_charm(
        peers=peers,
        _get_admin_api=Mock(return_value=admin_api),
    )
    fake._operator_user = "operator"
    fake._operator_password = "operator-password"

    charm.RabbitMQOperatorCharm._initialize_operator_user(fake)

    admin_api.create_user.assert_called_once_with(
        "operator",
        "operator-password",
        tags=["administrator"],
    )
    admin_api.create_user_permission.assert_called_once_with(
        "operator", vhost="/"
    )
    peers.set_operator_user_created.assert_called_once_with("operator")
    admin_api.delete_user.assert_called_once_with("guest")


def test_create_amqp_credentials_defers_without_bind_address():
    """AMQP credential creation waits for the peer bind address."""
    relation = SimpleNamespace(data={object(): {}})
    event = SimpleNamespace(relation=relation, defer=Mock())
    fake = _fake_charm(peers_bind_address=None, app=next(iter(relation.data)))

    charm.RabbitMQOperatorCharm.create_amqp_credentials(
        fake, event, "svc-user", "svc-vhost", False
    )

    event.defer.assert_called_once_with()


def test_create_amqp_credentials_success():
    """AMQP credentials are created and published onto the relation."""
    app = object()
    relation = SimpleNamespace(data={app: {}})
    event = SimpleNamespace(relation=relation, defer=Mock())
    peers = SimpleNamespace(
        store_password=Mock(),
        retrieve_password=Mock(return_value="stored-password"),
    )
    fake = _fake_charm(app=app, peers=peers)
    fake.does_vhost_exist.return_value = False
    fake.does_user_exist.return_value = False

    charm.RabbitMQOperatorCharm.create_amqp_credentials(
        fake, event, "svc-user", "svc-vhost", True
    )

    fake.create_vhost.assert_called_once_with("svc-vhost")
    fake.create_user.assert_called_once_with("svc-user")
    peers.store_password.assert_called_once_with("svc-user", "new-password")
    fake.set_user_permissions.assert_called_once_with("svc-user", "svc-vhost")
    assert relation.data[app] == {
        "password": "stored-password",
        "hostname": "rabbitmq-k8s-endpoints",
    }


def test_create_amqp_credentials_defers_on_http_401():
    """Unauthorized AMQP credential creation is deferred for later retry."""
    app = object()
    relation = SimpleNamespace(data={app: {}})
    event = SimpleNamespace(relation=relation, defer=Mock())
    error = requests.exceptions.HTTPError("unauthorized")
    error.response = SimpleNamespace(status_code=401)
    fake = _fake_charm(
        app=app,
        does_vhost_exist=Mock(side_effect=error),
    )

    charm.RabbitMQOperatorCharm.create_amqp_credentials(
        fake, event, "svc-user", "svc-vhost", False
    )

    event.defer.assert_called_once_with()


def test_get_service_account_fails_when_rabbit_unavailable():
    """Service-account action fails if RabbitMQ and peer binding are both missing."""
    event = Mock()
    event.params = {"username": "svc-user", "vhost": "svc-vhost"}
    fake = _fake_charm(
        peers_bind_address=None,
        peers=SimpleNamespace(retrieve_password=Mock()),
    )
    fake.rabbit_running = False

    charm.RabbitMQOperatorCharm._get_service_account(fake, event)

    event.fail.assert_called_once_with(
        "RabbitMQ not running, unable to create account"
    )


def test_get_service_account_success():
    """Service-account action returns connection details on success."""
    event = Mock()
    event.params = {"username": "svc-user", "vhost": "svc-vhost"}
    peers = SimpleNamespace(
        store_password=Mock(),
        retrieve_password=Mock(return_value="svc-password"),
    )
    fake = _fake_charm(peers=peers)
    fake.rabbit_running = True
    fake.ingress_address = "10.5.0.1"
    fake.rabbitmq_url = Mock(return_value="rabbit://svc-user:svc-password")
    fake.does_vhost_exist.return_value = False
    fake.does_user_exist.return_value = False

    charm.RabbitMQOperatorCharm._get_service_account(fake, event)

    event.set_results.assert_called_once_with(
        {
            "username": "svc-user",
            "password": "svc-password",
            "vhost": "svc-vhost",
            "ingress-address": "10.5.0.1",
            "port": 5672,
            "url": "rabbit://svc-user:svc-password",
        }
    )


def test_ensure_queue_ha_raises_without_enough_nodes():
    """Queue HA fails when the cluster is too small for the configured replicas."""
    admin_api = Mock()
    admin_api.list_nodes.return_value = [{"name": "node1"}, {"name": "node2"}]
    fake = _fake_charm(
        get_undersized_queues=Mock(return_value=[{"name": "q1"}]),
        _get_admin_api=Mock(return_value=admin_api),
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm.ensure_queue_ha(fake)


def test_ensure_queue_ha_rebalances_when_replicated():
    """Queue HA rebalances once replicas were added."""
    admin_api = Mock()
    admin_api.list_nodes.return_value = [
        {"name": "node1"},
        {"name": "node2"},
        {"name": "node3"},
    ]
    undersized_queues = [{"name": "q1", "members": ["node1"], "vhost": "/"}]
    fake = _fake_charm(
        get_undersized_queues=Mock(return_value=undersized_queues),
        _get_admin_api=Mock(return_value=admin_api),
        _add_members_to_undersized_queues=Mock(return_value=["q1"]),
    )

    result = charm.RabbitMQOperatorCharm.ensure_queue_ha(fake)

    assert result == {"undersized-queues": 1, "replicated-queues": 1}
    admin_api.rebalance_queues.assert_called_once_with()


@pytest.mark.parametrize(
    (
        "leader",
        "rabbit_running",
        "operator_user_created",
        "manage_queues",
        "message",
    ),
    [
        (
            False,
            True,
            "rmqadmin",
            True,
            "Not leader unit, unable to ensure queue HA",
        ),
        (
            True,
            False,
            "rmqadmin",
            True,
            "RabbitMQ not running, unable to ensure queue HA",
        ),
        (
            True,
            True,
            None,
            True,
            "Operator user not created, unable to ensure queue HA",
        ),
        (
            True,
            True,
            "rmqadmin",
            False,
            "Queue management is disabled, unable to ensure queue HA",
        ),
    ],
)
def test_ensure_queue_ha_action_gate_failures(
    leader, rabbit_running, operator_user_created, manage_queues, message
):
    """Queue HA action fails early for each gating condition."""
    event = Mock()
    event.params = {"dry-run": False}
    unit = Mock()
    unit.is_leader.return_value = leader
    fake = _fake_charm(
        unit=unit,
        peers=SimpleNamespace(operator_user_created=operator_user_created),
        _manage_queues=Mock(return_value=manage_queues),
    )
    fake.rabbit_running = rabbit_running

    charm.RabbitMQOperatorCharm._ensure_queue_ha_action(fake, event)

    event.fail.assert_called_once_with(message)


def test_ensure_queue_ha_action_success():
    """Queue HA action returns the replication summary and updates status."""
    event = Mock()
    event.params = {"dry-run": True}
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        unit=unit,
        peers=SimpleNamespace(operator_user_created="rmqadmin"),
        _manage_queues=Mock(return_value=True),
        ensure_queue_ha=Mock(
            return_value={
                "undersized-queues": 2,
                "replicated-queues": 1,
            }
        ),
        _on_update_status=Mock(),
    )
    fake.rabbit_running = True

    charm.RabbitMQOperatorCharm._ensure_queue_ha_action(fake, event)

    event.set_results.assert_called_once_with(
        {
            "undersized-queues": 2,
            "replicated-queues": 1,
            "dry-run": True,
        }
    )
    fake._on_update_status.assert_called_once_with(event)


def test_get_hostname_prefers_loadbalancer_when_requested():
    """External connectivity prefers the load-balancer IP when available."""
    fake = _fake_charm(
        hostname="rabbitmq-k8s-endpoints.model.svc.cluster.local",
        _get_loadbalancer_ip=Mock(return_value="203.0.113.10"),
    )

    hostname = charm.RabbitMQOperatorCharm.get_hostname(
        fake, external_connectivity=True
    )

    assert hostname == "203.0.113.10"


def test_get_loadbalancer_ip_returns_none_on_apierror():
    """Load-balancer lookup failures are handled gracefully."""

    class _HashableCharm(SimpleNamespace):
        __hash__ = object.__hash__

    request = httpx.Request("GET", "https://kubernetes.invalid")
    response = httpx.Response(
        404,
        request=request,
        json={
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": "not found",
            "reason": "NotFound",
            "code": 404,
        },
    )
    fake = _HashableCharm(
        **_fake_charm(
            lightkube_client=SimpleNamespace(
                get=Mock(
                    side_effect=charm.ApiError(
                        request=request, response=response
                    )
                )
            ),
            model=SimpleNamespace(name="test-model"),
            _lb_name="rabbitmq-k8s-lb",
        ).__dict__
    )

    result = charm.RabbitMQOperatorCharm._get_loadbalancer_ip(fake)

    assert result is None


def test_reconcile_lb_sets_blocked_on_invalid_annotations():
    """Invalid load-balancer annotations block the unit."""
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        unit=unit,
        _annotations_valid=False,
        _get_lb_resource_manager=Mock(
            return_value=SimpleNamespace(reconcile=Mock())
        ),
    )

    charm.RabbitMQOperatorCharm._reconcile_lb(fake, None)

    assert unit.status == ops.model.BlockedStatus(
        "Invalid config value 'loadbalancer_annotations'"
    )
    fake._get_lb_resource_manager.return_value.reconcile.assert_called_once_with(
        []
    )


def test_reconcile_lb_reconciles_valid_service():
    """Valid annotations produce a load-balancer reconciliation request."""
    unit = Mock()
    unit.is_leader.return_value = True
    resource_manager = SimpleNamespace(reconcile=Mock())
    service = Mock()
    fake = _fake_charm(
        unit=unit,
        _annotations_valid=True,
        _construct_lb=Mock(return_value=service),
        _get_lb_resource_manager=Mock(return_value=resource_manager),
        _lb_name="rabbitmq-k8s-lb",
    )

    charm.RabbitMQOperatorCharm._reconcile_lb(fake, None)

    resource_manager.reconcile.assert_called_once_with([service])
