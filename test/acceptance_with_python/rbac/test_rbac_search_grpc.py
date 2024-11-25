import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_rbac_search(request: SubRequest, admin_client, custom_client):
    name_collection1 = _sanitize_role_name(request.node.name) + "col1"
    name_collection2 = _sanitize_role_name(request.node.name) + "col2"
    admin_client.collections.delete([name_collection1, name_collection2])
    name_role = _sanitize_role_name(request.node.name) + "role"
    admin_client.roles.delete(name_role)

    col1 = admin_client.collections.create(name=name_collection1)
    col1.data.insert({})

    col2 = admin_client.collections.create(name=name_collection2)
    col2.data.insert({})

    # with correct rights
    admin_client.roles.create(
        name=name_role,
        permissions=[
            RBAC.permissions.config.read(collection=col1.name),
            RBAC.permissions.data.read(collection=col1.name),
        ],
    )
    admin_client.roles.assign(user="custom-user", roles=name_role)

    col_no_rights = custom_client.collections.get(col1.name)  # no network call => no RBAC check

    res = col_no_rights.query.fetch_objects()
    assert len(res.objects) == 1

    admin_client.roles.revoke(user="custom-user", roles=name_role)
    admin_client.roles.delete(name_role)

    # with unrelated rights
    admin_client.roles.create(
        name=name_role,
        permissions=RBAC.permissions.roles.read(),
    )
    admin_client.roles.assign(user="custom-user", roles=name_role)

    col_no_rights = custom_client.collections.get(col1.name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.WeaviateQueryException) as e:
        col_no_rights.query.fetch_objects()
    assert "forbidden" in e.value.args[0]
    admin_client.roles.revoke(user="custom-user", roles=name_role)
    admin_client.roles.delete(name_role)

    # rights for wrong collection
    admin_client.roles.create(
        name=name_role,
        permissions=RBAC.permissions.config.read(
            collection=col2.name,
        ),
    )
    admin_client.roles.assign(user="custom-user", roles=name_role)

    col_no_rights = custom_client.collections.get(col1.name)  # no network call => no RBAC check

    with pytest.raises(weaviate.exceptions.WeaviateQueryException) as e:
        col_no_rights.query.fetch_objects()
    assert "forbidden" in e.value.args[0]
    admin_client.roles.revoke(user="custom-user", roles=name_role)
    admin_client.roles.delete(name_role)

    # only metadata rights
    admin_client.roles.create(
        name=name_role,
        permissions=RBAC.permissions.config.read(
            collection=col1.name,
        ),
    )
    admin_client.roles.assign(user="custom-user", roles=name_role)

    col_no_rights = custom_client.collections.get(col1.name)  # no network call => no RBAC check

    with pytest.raises(weaviate.exceptions.WeaviateQueryException) as e:
        col_no_rights.query.fetch_objects()
    assert "forbidden" in e.value.args[0]

    admin_client.roles.revoke(user="custom-user", roles=name_role)
    admin_client.roles.delete(name_role)

    # only data rights
    admin_client.roles.create(
        name=name_role,
        permissions=RBAC.permissions.data.read(collection=col1.name),
    )
    admin_client.roles.assign(user="custom-user", roles=name_role)

    col_no_rights = custom_client.collections.get(col1.name)  # no network call => no RBAC check

    with pytest.raises(weaviate.exceptions.WeaviateQueryException) as e:
        col_no_rights.query.fetch_objects()
    assert "forbidden" in e.value.args[0]

    admin_client.roles.revoke(user="custom-user", roles=name_role)
    admin_client.roles.delete(name_role)

    admin_client.collections.delete([name_collection1, name_collection2])
