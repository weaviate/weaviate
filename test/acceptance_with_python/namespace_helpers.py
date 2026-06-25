"""Client-based control-plane helpers for the namespace acceptance tests.

These wrap the weaviate-python-client namespaces/users/roles APIs (added in
weaviate-python-client#2033) with the RAFT-apply-lag retries the 3-node
namespaces cluster needs:

  - a freshly created namespace must become visible before users can reference it,
  - a namespaced user create can transiently 422 ("namespace does not exist")
    until the create-namespace FSM entry applies locally,
  - a freshly created apikey can 401 on a follower still replicating.

Used by test_namespace_refs.py and test_namespace_nodes.py. Control-plane setup
goes through these (the qualified-id namespace surface + the retries); the data
operations under test go through the client directly.
"""

import time
from typing import List, Optional, Tuple

import weaviate
import weaviate.classes as wvc
from weaviate import WeaviateClient
from weaviate.exceptions import UnexpectedStatusCodeError
from weaviate.rbac.models import Permissions


def _status(err: UnexpectedStatusCodeError) -> Optional[int]:
    return getattr(err, "status_code", None)


def open_client(
    key: str, http_port: int, grpc_port: int, *, skip_init_checks: bool = True
) -> WeaviateClient:
    """Open a local client authenticated as key. skip_init_checks lets restricted
    users (viewer, no-role) connect without the init-time meta/gRPC probes they
    may lack permission for."""
    return weaviate.connect_to_local(
        port=http_port,
        grpc_port=grpc_port,
        auth_credentials=wvc.init.Auth.api_key(key),
        skip_init_checks=skip_init_checks,
    )


def create_namespace(admin: WeaviateClient, name: str, *, timeout: float = 10.0) -> None:
    """Create the namespace (409 = already exists) and poll until it is visible."""
    try:
        admin.namespaces.create(name=name)
    except UnexpectedStatusCodeError as e:
        if _status(e) != 409:
            raise
    deadline = time.time() + timeout
    while time.time() < deadline:
        if admin.namespaces.get(name=name) is not None:
            return
        time.sleep(0.05)
    raise AssertionError(f"namespace {name!r} not visible within {timeout}s")


def create_user(admin: WeaviateClient, qualified_id: str, *, timeout: float = 10.0) -> str:
    """Create a namespace-qualified db user ("<ns>:<user>"), returning its apikey.

    Absorbs a stale 409 (delete + retry) and the 422 "namespace does not exist"
    window where the local FSM hasn't applied the create-namespace entry yet.
    """
    deadline = time.time() + timeout
    deleted = False
    last = ""
    while time.time() < deadline:
        try:
            return admin.users.db.create(user_id=qualified_id)
        except UnexpectedStatusCodeError as e:
            code = _status(e)
            if code == 409 and not deleted:
                try:
                    admin.users.db.delete(user_id=qualified_id)
                except UnexpectedStatusCodeError:
                    pass
                deleted = True
                deadline = time.time() + timeout
            elif code == 422 and "does not exist" in str(e):
                last = str(e)
                time.sleep(0.05)
            else:
                raise
    raise AssertionError(f"could not create user {qualified_id!r} within {timeout}s: {last}")


def delete_user(admin: WeaviateClient, qualified_id: str) -> None:
    try:
        admin.users.db.delete(user_id=qualified_id)
    except UnexpectedStatusCodeError:
        pass


def assign_role(admin: WeaviateClient, qualified_id: str, role: str) -> None:
    admin.users.db.assign_roles(user_id=qualified_id, role_names=role)


def create_verbose_nodes_role(admin: WeaviateClient, role_name: str) -> None:
    """Create a custom role granting verbose read_nodes over all collections; the
    matcher scopes it to the assignee's namespace. Idempotent across re-runs."""
    try:
        admin.roles.delete(role_name)
    except UnexpectedStatusCodeError:
        pass
    admin.roles.create(
        role_name=role_name,
        permissions=Permissions.Nodes.verbose(collection="*", read=True),
    )


def delete_role(admin: WeaviateClient, role_name: str) -> None:
    try:
        admin.roles.delete(role_name)
    except UnexpectedStatusCodeError:
        pass


def wait_for_key(key: str, nodes: List[Tuple[int, int]], *, timeout: float = 10.0) -> None:
    """Poll get-my-user on every node until the RAFT-forwarded create is applied.

    The test client may talk to a follower still replicating; checking every node
    pins the apikey before the next authenticated request would transiently 401.
    """
    for http_port, grpc_port in nodes:
        c = open_client(key, http_port, grpc_port)
        try:
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    c.users.get_my_user()
                    break
                except Exception:
                    time.sleep(0.1)
            else:
                raise AssertionError(f"apikey not recognized on node {http_port} within {timeout}s")
        finally:
            c.close()
