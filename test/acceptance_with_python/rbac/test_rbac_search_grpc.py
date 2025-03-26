import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import Permissions
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, RoleWrapperProtocol, generate_missing_permissions

pytestmark = pytest.mark.xdist_group(name="rbac")


@pytest.mark.parametrize("mt", [True, False])
def test_rbac_search(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name_collection1 = _sanitize_role_name(request.node.name) + "col1"
    name_collection2 = _sanitize_role_name(request.node.name) + "col2"
    admin_client.collections.delete([name_collection1, name_collection2])
    name_role = _sanitize_role_name(request.node.name) + "role"
    admin_client.roles.delete(name_role)

    col1 = admin_client.collections.create(
        name=name_collection1, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    col2 = admin_client.collections.create(
        name=name_collection2, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )

    if mt:
        col1.tenants.create("tenant1")
        col2.tenants.create("tenant1")
        col1 = col1.with_tenant("tenant1")
        col2 = col2.with_tenant("tenant1")

    col1.data.insert({})
    col2.data.insert({})

    # with correct rights
    required_permissions = [
        Permissions.data(collection=col1.name, read=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        col_no_rights = custom_client.collections.get(col1.name)  # no network call => no RBAC check
        if mt:
            col_no_rights = col_no_rights.with_tenant("tenant1")
        res = col_no_rights.query.fetch_objects()
        assert len(res.objects) == 1

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            col_no_rights = custom_client.collections.get(
                col1.name
            )  # no network call => no RBAC check
            if mt:
                col_no_rights = col_no_rights.with_tenant("tenant1")

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                col_no_rights.query.fetch_objects()
            assert e.value.status_code == 7

    # rights for wrong collection
    wrong_collection = Permissions.collections(collection=col2.name, read_config=True)
    with role_wrapper(admin_client, request, wrong_collection):
        col_no_rights = custom_client.collections.get(col1.name)  # no network call => no RBAC check
        if mt:
            col_no_rights = col_no_rights.with_tenant("tenant1")
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            col_no_rights.query.fetch_objects()
        assert e.value.status_code == 7
