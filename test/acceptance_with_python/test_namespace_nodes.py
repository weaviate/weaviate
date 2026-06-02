"""Namespace-scoped /v1/nodes through the weaviate-python client.

Mirrors test/acceptance/namespace/nodes_test.go, but exercises the
python-client `cluster.nodes()` deserialization under namespace RBAC scoping —
the client surface the Go acceptance test doesn't reach:

  - a namespaced admin sees verbose node/shard info for its own collections only,
  - the node-wide minimal view stays denied (403),
  - a regular namespace viewer sees no shards,
  - a custom role with verbose read_nodes grants scoped access to a
    non-admin namespace user,
  - the global root sees every namespace's shards.

Control-plane setup (namespaces, namespaced users, role grants) goes through the
client's namespaces/users/roles APIs wrapped with the RAFT-lag retries in
namespace_helpers; the nodes reads under test go through the client too.

CI: runs in the "python-namespaces" matrix entry (see test_namespace_refs.py).
Manual run:
    docker compose -f docker-compose-namespaces-test.yml up -d
    pytest test/acceptance_with_python/test_namespace_nodes.py
"""

import time
from typing import Callable, Dict, Iterator, List, Set, Tuple

import pytest
import weaviate.classes.config as wvcc
from weaviate import WeaviateClient
from weaviate.exceptions import UnexpectedStatusCodeError

import namespace_helpers as nsh

# Ports must match docker-compose-namespaces-test.yml.
NODES: List[Tuple[int, int]] = [
    (8190, 50190),
    (8191, 50191),
    (8192, 50192),
]
ADMIN_KEY = "admin-key"
NS1 = "customer1"
NS2 = "customer2"
PROBE = "NodesProbe"

# One xdist worker: the module fixture creates shared namespaces/users; parallel
# workers would race the 409→delete→recreate fallback. Same rationale as
# test_namespace_refs.py.
pytestmark = pytest.mark.xdist_group(name="namespace_nodes")


@pytest.fixture(scope="module")
def keys() -> Iterator[Dict[str, str]]:
    """Create both namespaces and the cast of users, returning their apikeys.

    - admin1/admin2: built-in admin in customer1/customer2 (the namespace admins),
    - viewer1: built-in viewer in customer1 (a regular namespace user),
    - bare1: no role in customer1 (granted a custom verbose-nodes role at runtime by a test).
    """
    cast = {
        "admin1": (f"{NS1}:admin1", "admin"),
        "admin2": (f"{NS2}:admin2", "admin"),
        "viewer1": (f"{NS1}:viewer1", "viewer"),
        "bare1": (f"{NS1}:bare1", None),
    }
    admin = nsh.open_client(ADMIN_KEY, *NODES[0], skip_init_checks=False)
    out: Dict[str, str] = {}
    try:
        nsh.create_namespace(admin, NS1)
        nsh.create_namespace(admin, NS2)
        for label, (qualified, role) in cast.items():
            out[label] = nsh.create_user(admin, qualified)
            if role is not None:
                nsh.assign_role(admin, qualified, role)
    finally:
        admin.close()
    for k in out.values():
        nsh.wait_for_key(k, NODES)
    yield out


@pytest.fixture
def open_client() -> Iterator[Callable[..., WeaviateClient]]:
    """Open clients (node 0 by default), closing them all at teardown."""
    opened: List[WeaviateClient] = []

    def _open(key: str, node: int = 0, skip_init_checks: bool = True) -> WeaviateClient:
        http_port, grpc_port = NODES[node]
        c = nsh.open_client(key, http_port, grpc_port, skip_init_checks=skip_init_checks)
        opened.append(c)
        return c

    try:
        yield _open
    finally:
        for c in opened:
            try:
                c.close()
            except Exception:
                pass


@pytest.fixture(scope="module")
def probe_collections(keys: Dict[str, str]) -> Iterator[None]:
    """Create the short class PROBE in both namespaces (stored qualified) so each
    namespace has at least one shard for the nodes endpoint to report."""
    for key in (keys["admin1"], keys["admin2"]):
        c = nsh.open_client(key, *NODES[0])
        try:
            c.collections.delete(PROBE)
            c.collections.create(
                PROBE,
                properties=[wvcc.Property(name="title", data_type=wvcc.DataType.TEXT)],
            )
        finally:
            c.close()
    yield
    # Best-effort cleanup via root, using the qualified names.
    root = nsh.open_client(ADMIN_KEY, *NODES[0], skip_init_checks=False)
    try:
        for ns in (NS1, NS2):
            try:
                root.collections.delete(f"{ns}:{PROBE}")
            except Exception:
                pass
    finally:
        root.close()


def _shard_namespaces(nodes_list) -> Tuple[Set[str], int]:
    """Return the set of `<ns>:` prefixes across all shards plus the shard count."""
    prefixes: Set[str] = set()
    total = 0
    for node in nodes_list:
        for shard in node.shards or []:
            if ":" in shard.collection:
                prefixes.add(shard.collection.split(":", 1)[0] + ":")
            total += 1
    return prefixes, total


def _assert_scoped_to(nodes_list, want_ns: str) -> None:
    """Every shard belongs to want_ns, at least one is present, and each node's
    aggregate matches the returned (scoped) shards over the wire — the leak-fix
    invariant: a node-wide Stats spanning other namespaces would break it."""
    prefixes, total = _shard_namespaces(nodes_list)
    assert total > 0, "scoped caller must see at least one of its own shards"
    assert prefixes == {want_ns}, f"verbose nodes leaked outside {want_ns!r}: saw {prefixes}"
    for node in nodes_list:
        if node.stats is None:
            continue
        objects = sum(s.object_count for s in (node.shards or []))
        assert node.stats.shard_count == len(node.shards or [])
        assert node.stats.object_count == objects


def _assert_minimal_forbidden(client: WeaviateClient) -> None:
    with pytest.raises(UnexpectedStatusCodeError) as ei:
        client.cluster.nodes(output="minimal")
    assert getattr(ei.value, "status_code", None) == 403 or "403" in str(ei.value)


def test_namespaced_admin_sees_only_its_collections(keys, probe_collections, open_client):
    client = open_client(keys["admin1"])
    _assert_scoped_to(client.cluster.nodes(output="verbose"), f"{NS1}:")


def test_namespaced_admin_denied_minimal(keys, probe_collections, open_client):
    _assert_minimal_forbidden(open_client(keys["admin1"]))


def test_regular_namespace_viewer_sees_no_shards(keys, probe_collections, open_client):
    client = open_client(keys["viewer1"])
    _, total = _shard_namespaces(client.cluster.nodes(output="verbose"))
    assert total == 0, "a namespace viewer without a nodes grant must see no shards"
    _assert_minimal_forbidden(client)


def test_custom_verbose_nodes_role_grants_scoped_access(keys, probe_collections, open_client):
    client = open_client(keys["bare1"])

    # No role yet: verbose returns 200 with no shards; minimal is denied.
    _, total = _shard_namespaces(client.cluster.nodes(output="verbose"))
    assert total == 0, "a bare namespace user must see no shards before the role is granted"
    _assert_minimal_forbidden(client)

    # No built-in nodes role for non-admin namespace users; an operator grants a
    # custom role with verbose read_nodes over all collections — the matcher
    # scopes it to the caller's namespace.
    admin = nsh.open_client(ADMIN_KEY, *NODES[0], skip_init_checks=False)
    try:
        nsh.create_verbose_nodes_role(admin, "ns-nodes-viewer")
        nsh.assign_role(admin, f"{NS1}:bare1", "ns-nodes-viewer")
    finally:
        admin.close()

    # Poll until the role applies on the node this client talks to.
    deadline = time.time() + 20.0
    while time.time() < deadline:
        _, total = _shard_namespaces(client.cluster.nodes(output="verbose"))
        if total > 0:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("custom verbose-nodes role never exposed the namespace's shards")

    _assert_scoped_to(client.cluster.nodes(output="verbose"), f"{NS1}:")
    # verbose-only role: the node-wide minimal view stays denied.
    _assert_minimal_forbidden(client)


def test_global_root_sees_every_namespace(keys, probe_collections, open_client):
    client = open_client(ADMIN_KEY, skip_init_checks=False)
    prefixes, total = _shard_namespaces(client.cluster.nodes(output="verbose"))
    assert total > 0
    assert {f"{NS1}:", f"{NS2}:"} <= prefixes, f"root must see both namespaces; saw {prefixes}"
