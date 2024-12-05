from typing import (
    Union,
    Sequence,
    Any,
    ContextManager,
    Protocol,
    Iterator,
    List,
)

import pytest
import weaviate
import weaviate.classes as wvc
from _pytest.fixtures import SubRequest
from contextlib import contextmanager

from weaviate import WeaviateClient
from weaviate.rbac.models import PermissionsType, _Permission


def _sanitize_role_name(name: str) -> str:
    return (
        name.replace("[", "")
        .replace("]", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .replace("{", "")
        .replace("}", "")
    )


def generate_missing_permissions(permissions: PermissionsType):
    result = []
    permissions = _flatten_permissions(permissions)
    for i in range(len(permissions)):
        result.append(permissions[:i] + permissions[i + 1 :])
    return result


def _flatten_permissions(permissions: PermissionsType) -> List[_Permission]:
    if isinstance(permissions, _Permission):
        return [permissions]
    flattened_permissions: List[_Permission] = []
    for permission in permissions:
        if isinstance(permission, _Permission):
            flattened_permissions.append(permission)
        else:
            flattened_permissions.extend(permission)
    return flattened_permissions


class RoleWrapperProtocol(Protocol):
    def __call__(
        self,
        admin_client: WeaviateClient,
        request: SubRequest,
        permissions: PermissionsType,
        user: str = "custom-user",
    ) -> ContextManager[Any]: ...


@pytest.fixture
def role_wrapper() -> RoleWrapperProtocol:
    def wrapper(
        admin_client: WeaviateClient,
        request: SubRequest,
        permissions: PermissionsType,
        user: str = "custom-user",
    ) -> Iterator[None]:
        name = _sanitize_role_name(request.node.name) + "role"
        admin_client.roles.delete(name)
        if not isinstance(permissions, list) or len(permissions) > 0:
            admin_client.roles.create(name=name, permissions=permissions)
            admin_client.roles.assign(user=user, roles=name)

        yield

        if not isinstance(permissions, list) or len(permissions) > 0:
            admin_client.roles.revoke(user=user, roles=name)
            admin_client.roles.delete(name)

    return contextmanager(wrapper)


@pytest.fixture
def admin_client():
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        yield client


@pytest.fixture
def custom_client():
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
    ) as client:
        yield client


@pytest.fixture
def viewer_client():
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("viewer-key")
    ) as client:
        yield client
