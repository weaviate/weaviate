import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC

from .conftest import _sanitize_role_name
from _pytest.fixtures import SubRequest

pytestmark = pytest.mark.xdist_group(name="rbac")


@pytest.mark.parametrize("to_upper", [True, False])
def test_rbac_refs(request: SubRequest, admin_client, to_upper: bool):
    name = _sanitize_role_name(request.node.name)
    if to_upper:
        name = name[0].upper() + name[1:]
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    collection = admin_client.collections.create(name=name)

    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
    ) as client_no_rights:
        admin_client.roles.create(
            name=name,
            permissions=[
                RBAC.permissions.config.read(collection=name),
                RBAC.permissions.data.read(collection=name),
            ],
        )
        admin_client.roles.assign(user="custom-user", roles=name)
        collection_no_rights = client_no_rights.collections.get(collection.name)
        collection_no_rights.query.fetch_objects()

        admin_client.roles.revoke(user="custom-user", roles=name)
        admin_client.roles.delete(name)

    admin_client.collections.delete(name)
