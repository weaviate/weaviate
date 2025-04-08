from typing import (
    Union,
    Sequence,
    Any,
    ContextManager,
    Protocol,
    Iterator,
)

import pytest
import weaviate
import weaviate.classes as wvc
from _pytest.fixtures import SubRequest
from contextlib import contextmanager

from weaviate import WeaviateClient
from weaviate.rbac.models import PermissionsCreateType


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


def generate_missing_permissions(permissions: list):
    result = []
    for i in range(len(permissions)):
        result.append(permissions[:i] + permissions[i + 1 :])
    return result


class RoleWrapperProtocol(Protocol):
    def __call__(
        self,
        admin_client: WeaviateClient,
        request: SubRequest,
        permissions: PermissionsCreateType,
        user: str = "custom-user",
    ) -> ContextManager[Any]: ...


@pytest.fixture
def role_wrapper() -> RoleWrapperProtocol:
    def wrapper(
        admin_client: WeaviateClient,
        request: SubRequest,
        permissions: PermissionsCreateType,
        user: str = "custom-user",
    ) -> Iterator[None]:
        name = _sanitize_role_name(request.node.name) + "role"
        admin_client.roles.delete(name)
        if not isinstance(permissions, list) or len(permissions) > 0:
            admin_client.roles.create(role_name=name, permissions=permissions)
            admin_client.users.assign_roles(user_id=user, role_names=name)

        yield

        if not isinstance(permissions, list) or len(permissions) > 0:
            admin_client.users.revoke_roles(user_id=user, role_names=name)
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
