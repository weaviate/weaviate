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

Control-plane setup (namespaces, namespaced users, role grants) goes through
raw REST — the python client has no namespace API and these need RAFT-lag
retries; the nodes reads under test go through the client.

CI: runs in the "python-namespaces" matrix entry (see test_namespace_refs.py).
Manual run:
    docker compose -f docker-compose-namespaces-test.yml up -d
    pytest test/acceptance_with_python/test_namespace_nodes.py
"""

import json as _json
import time
import urllib.error
import urllib.request
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple

import pytest
import weaviate
import weaviate.classes as wvc
import weaviate.classes.config as wvcc
from weaviate import WeaviateClient
from weaviate.exceptions import UnexpectedStatusCodeError

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

# One xdist worker: the module fixture creates shared namespaces/users via REST;
# parallel workers would race the 409→delete→recreate fallback. Same rationale
# as test_namespace_refs.py.
pytestmark = pytest.mark.xdist_group(name="namespace_nodes")


def _rest_base(http_port: int) -> str:
    return f"http://localhost:{http_port}/v1"


def _admin_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {ADMIN_KEY}", "Content-Type": "application/json"}


def _http(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[dict] = None,
) -> Tuple[int, bytes]:
    """stdlib-only HTTP call returning (status_code, body); never raises on 4xx."""
    data = _json.dumps(json_body).encode() if json_body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=dict(headers or {}))
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() or b""


def _create_namespace(http_port: int, name: str) -> None:
    """POST /namespaces/{name} (409 = already exists), then poll until visible."""
    status, body = _http("POST", f"{_rest_base(http_port)}/namespaces/{name}", _admin_headers())
    assert status in (201, 409), f"create namespace {name}: {status} {body!r}"
    deadline = time.time() + 10.0
    while time.time() < deadline:
        status, _ = _http("GET", f"{_rest_base(http_port)}/namespaces/{name}", _admin_headers())
        if status == 200:
            return
        time.sleep(0.05)
    raise AssertionError(f"namespace {name!r} not visible within 10s")


def _assign_role(http_port: int, qualified_user: str, role: str) -> None:
    status, body = _http(
        "POST",
        f"{_rest_base(http_port)}/authz/users/{qualified_user}/assign",
        _admin_headers(),
        {"roles": [role], "userType": "db"},
    )
    assert status in (200, 201), f"assign {role} to {qualified_user}: {status} {body!r}"


def _create_role(http_port: int, name: str, permissions: List[dict]) -> None:
    """POST /authz/roles; on 409 (already exists) delete and retry so re-runs are idempotent."""
    url = f"{_rest_base(http_port)}/authz/roles"
    body_in = {"name": name, "permissions": permissions}
    status, body = _http("POST", url, _admin_headers(), body_in)
    if status == 409:
        _http("DELETE", f"{url}/{name}", _admin_headers())
        status, body = _http("POST", url, _admin_headers(), body_in)
    assert status in (200, 201), f"create role {name}: {status} {body!r}"


def _create_user(http_port: int, qualified: str, role: Optional[str]) -> str:
    """POST /users/db/{qualified}, optionally grant a role, return its apikey.

    Absorbs a stale 409 (delete + retry) and the 422 "namespace does not exist"
    window where the local FSM hasn't applied the create-namespace entry yet.
    Mirrors _create_namespaced_user in test_namespace_refs.py.
    """
    url = f"{_rest_base(http_port)}/users/db/{qualified}"
    deleted = False
    deadline = time.time() + 10.0
    last = (0, b"")
    while time.time() < deadline:
        status, body = _http("POST", url, _admin_headers(), {})
        if status == 201:
            apikey = _json.loads(body).get("apikey")
            assert apikey, f"createUser returned no apikey: {body!r}"
            if role is not None:
                _assign_role(http_port, qualified, role)
            return apikey
        if status == 409 and not deleted:
            _http("DELETE", url, _admin_headers())
            deleted = True
            deadline = time.time() + 10.0
        elif status == 422 and b"does not exist" in body:
            last = (status, body)
            time.sleep(0.05)
        else:
            raise AssertionError(f"create user {qualified}: {status} {body!r}")
    raise AssertionError(f"could not create user {qualified} within 10s: {last}")


def _wait_for_key(key: str) -> None:
    """Poll own-info on every node until the RAFT-forwarded create is applied."""
    for http_port, _ in NODES:
        deadline = time.time() + 10.0
        while time.time() < deadline:
            status, _ = _http(
                "GET",
                f"{_rest_base(http_port)}/users/own-info",
                {"Authorization": f"Bearer {key}"},
            )
            if status == 200:
                break
            time.sleep(0.1)
        else:
            raise AssertionError(f"apikey not recognized on node {http_port} within 10s")


@pytest.fixture(scope="module")
def keys() -> Iterator[Dict[str, str]]:
    """Create both namespaces and the cast of users, returning their apikeys.

    - admin1/admin2: built-in admin in customer1/customer2 (the namespace admins),
    - viewer1: built-in viewer in customer1 (a regular namespace user),
    - bare1: no role in customer1 (granted a custom verbose-nodes role at runtime by a test).
    """
    http_port = NODES[0][0]
    _create_namespace(http_port, NS1)
    _create_namespace(http_port, NS2)
    out = {
        "admin1": _create_user(http_port, f"{NS1}:admin1", "admin"),
        "admin2": _create_user(http_port, f"{NS2}:admin2", "admin"),
        "viewer1": _create_user(http_port, f"{NS1}:viewer1", "viewer"),
        "bare1": _create_user(http_port, f"{NS1}:bare1", None),
    }
    for k in out.values():
        _wait_for_key(k)
    yield out


@pytest.fixture
def open_client() -> Iterator[Callable[..., WeaviateClient]]:
    """Open clients (node 0 by default), closing them all at teardown.

    skip_init_checks lets restricted users (viewer, no-role) connect without the
    init-time meta/gRPC probes they may lack permission for.
    """
    opened: List[WeaviateClient] = []

    def _open(key: str, node: int = 0, skip_init_checks: bool = True) -> WeaviateClient:
        http_port, grpc_port = NODES[node]
        c = weaviate.connect_to_local(
            port=http_port,
            grpc_port=grpc_port,
            auth_credentials=wvc.init.Auth.api_key(key),
            skip_init_checks=skip_init_checks,
        )
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
        c = weaviate.connect_to_local(
            port=NODES[0][0],
            grpc_port=NODES[0][1],
            auth_credentials=wvc.init.Auth.api_key(key),
            skip_init_checks=True,
        )
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
    root = weaviate.connect_to_local(
        port=NODES[0][0], grpc_port=NODES[0][1], auth_credentials=wvc.init.Auth.api_key(ADMIN_KEY)
    )
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
    _create_role(
        NODES[0][0],
        "ns-nodes-viewer",
        [{"action": "read_nodes", "nodes": {"verbosity": "verbose", "collection": "*"}}],
    )
    _assign_role(NODES[0][0], f"{NS1}:bare1", "ns-nodes-viewer")

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
