from typing import Union, List

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC, DatabaseAction, CollectionsAction
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name

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


def test_rbac_users(request: SubRequest):
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        database_permissions = RBAC.actions.database.CREATE_COLLECTIONS
        num_roles = 2
        role_names = [_sanitize_role_name(request.node.name) + str(i) for i in range(num_roles)]
        for role_name in role_names:
            client.roles.delete(role_name)
            client.roles.create(
                name=role_name,
                permissions=RBAC.permissions.database(database_permissions),
            )
            client.roles.assign(user="admin-user", roles=role_name)

        roles = client.roles.by_user("admin-user")
        all_returned_names = [role.name for role in roles]
        assert all(name in all_returned_names for name in role_names)

        for role_name in role_names:
            client.roles.delete(role_name)
