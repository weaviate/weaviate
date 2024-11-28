import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC

from .conftest import _sanitize_role_name
from _pytest.fixtures import SubRequest

pytestmark = pytest.mark.xdist_group(name="rbac")


@pytest.mark.parametrize("to_upper", [True, False])
def test_rbac_refs(request: SubRequest, admin_client, custom_client, to_upper: bool):
    name = _sanitize_role_name(request.node.name)
    if to_upper:
        name = name[0].upper() + name[1:]
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    collection = admin_client.collections.create(name=name)

    admin_client.roles.create(
        name=name,
        permissions=[
            RBAC.permissions.config.read(collection=name),
            RBAC.permissions.data.read(collection=name),
        ],
    )
    admin_client.roles.assign(user="custom-user", roles=name)
    collection_no_rights = custom_client.collections.get(collection.name)
    collection_no_rights.query.fetch_objects()

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    admin_client.collections.delete(name)


def test_role_name_case_sensitivity(request: SubRequest, admin_client):
    col_name = _sanitize_role_name(request.node.name)
    l_name = _sanitize_role_name(request.node.name)
    u_name = l_name[0].upper() + l_name[1:]

    admin_client.collections.delete(col_name)
    admin_client.roles.delete(l_name)
    admin_client.roles.delete(u_name)

    admin_client.roles.create(
        name=l_name, permissions=RBAC.permissions.config.read(collection="lower")
    )

    admin_client.roles.create(
        name=u_name, permissions=RBAC.permissions.config.read(collection="upper")
    )

    admin_client.roles.assign(user="custom-user", roles=[l_name, u_name])

    roles = admin_client.roles.by_user("custom-user")
    assert sorted(roles.keys()) == sorted([l_name, u_name])

    admin_client.roles.delete(l_name)
    admin_client.roles.delete(u_name)
