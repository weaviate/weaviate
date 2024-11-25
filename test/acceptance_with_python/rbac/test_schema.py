import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, Role_Wrapper_Type, generate_missing_lists

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_rbac_collection_create_permissions(
    admin_client, custom_client, role_wrapper: Role_Wrapper_Type, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    permissions = [
        RBAC.permissions.config.read(collection=name),
        RBAC.permissions.config.create(collection=name),
    ]
    with role_wrapper(admin_client, request, permissions):
        assert custom_client.collections.create(name=name) is not None
        admin_client.collections.delete(name)

    for permission in generate_missing_lists(permissions):
        with role_wrapper(admin_client, request, permission):
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                custom_client.collections.create(
                    name=name,
                    properties=[
                        wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)
                    ],
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]
    admin_client.collections.delete(name)
