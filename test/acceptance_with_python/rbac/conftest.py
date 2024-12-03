from typing import Union, Callable, Generator, List, Sequence, Any, ContextManager

import pytest
import weaviate
import weaviate.classes as wvc
from _pytest.fixtures import SubRequest
from contextlib import contextmanager, _GeneratorContextManager

from weaviate import WeaviateClient
from weaviate.rbac.models import _Permission


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


Role_Wrapper_Type = Callable[
    [
        Any,
        SubRequest,
        Union[_Permission, Sequence[_Permission], Sequence[Sequence[_Permission]]],
    ],
    ContextManager[Any],
]


@pytest.fixture
def role_wrapper() -> Role_Wrapper_Type:
    @contextmanager
    def wrapper(
        admin_client: WeaviateClient,
        request: SubRequest,
        permissions: Union[_Permission, Sequence[_Permission], Sequence[Sequence[_Permission]]],
        user: str = "custom-user",
    ) -> ContextManager[Any]:
        name = _sanitize_role_name(request.node.name) + "role"
        admin_client.roles.delete(name)
        if not isinstance(permissions, list) or len(permissions) > 0:
            admin_client.roles.create(name=name, permissions=permissions)
            admin_client.roles.assign(user=user, roles=name)

        yield

        if not isinstance(permissions, list) or len(permissions) > 0:
            admin_client.roles.revoke(user=user, roles=name)
            admin_client.roles.delete(name)

    return wrapper


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
