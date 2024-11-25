import pytest
import weaviate
import weaviate.classes as wvc
from _pytest.fixtures import SubRequest
from contextlib import contextmanager

from typing_extensions import Callable, Generator
from weaviate.rbac.models import _ConfigPermission


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


def generate_missing_lists(permissions: list):
    result = []
    for i in range(len(permissions)):
        result.append(permissions[:i] + permissions[i + 1 :])
    return result


Role_Wrapper_Type = Callable[..., Generator[None, None, None]]


@pytest.fixture
def role_wrapper() -> Role_Wrapper_Type:
    @contextmanager
    def wrapper(
        admin_client, request: SubRequest, permissions: _ConfigPermission
    ) -> Generator[None, None, None]:
        name = _sanitize_role_name(request.node.name) + "role"
        admin_client.roles.delete(name)
        admin_client.roles.create(name=name, permissions=permissions)
        admin_client.roles.assign(user="custom-user", roles=name)

        yield

        admin_client.roles.revoke(user="custom-user", roles=name)
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
