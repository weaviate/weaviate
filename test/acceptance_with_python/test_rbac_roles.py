from typing import Union, List

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC, DatabaseAction, CollectionsAction
from _pytest.fixtures import SubRequest

db = RBAC.actions.database
col = RBAC.actions.collection


@pytest.mark.parametrize(
    "database_permissions",
    [
        db.READ_COLLECTIONS,
        db.READ_ROLES,
        db.MANAGE_ROLES,
        [db.READ_COLLECTIONS, db.CREATE_COLLECTIONS],
    ],
)
@pytest.mark.parametrize(
    "collection_permissions",
    [
        col.CREATE_OBJECTS,
        col.CREATE_TENANTS,
        [col.CREATE_TENANTS, col.UPDATE_OBJECTS, col.DELETE_OBJECTS],
    ],
)
def test_rbac_roles_admin(
    request: SubRequest,
    database_permissions: Union[DatabaseAction, List[DatabaseAction]],
    collection_permissions: Union[CollectionsAction, List[CollectionsAction]],
) -> None:
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)
        client.roles.create(
            name=role_name,
            permissions=RBAC.permissions.database(database_permissions)
            + RBAC.permissions.collection("*", actions=collection_permissions),
        )
        role = client.roles.by_name(role_name)
        assert role is not None
        if not isinstance(database_permissions, list):
            database_permissions = [database_permissions]
        assert len(role.database_permissions) == len(database_permissions)
        assert sorted(role.database_permissions) == sorted(database_permissions)
        client.roles.delete(role_name)
        assert client.roles.by_name(role_name) is None


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
