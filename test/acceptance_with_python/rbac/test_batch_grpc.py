import pytest
import weaviate
import weaviate.classes as wvc
from typing_extensions import Optional
from weaviate.rbac.models import Permissions
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, generate_missing_permissions, RoleWrapperProtocol

pytestmark = pytest.mark.xdist_group(name="rbac")


@pytest.mark.parametrize("mt", [True, False])
def test_batch_grpc(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete([name + "1", name + "2"])

    col1 = admin_client.collections.create(
        name=name + "1", multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    col2 = admin_client.collections.create(
        name=name + "2", multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    tenant: Optional[str] = None
    if mt:
        tenant = "tenant1"
        col1.tenants.create(tenant)
        col2.tenants.create(tenant)
    admin_client.roles.delete(name)

    required_permissions = [
        Permissions.data(collection=col1.name, create=True, update=True),
        Permissions.data(collection=col2.name, create=True, update=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        with custom_client.batch.fixed_size() as batch:
            batch.add_object(collection=col1.name, properties={}, tenant=tenant)
            batch.add_object(collection=col2.name, properties={}, tenant=tenant)
        assert len(custom_client.batch.failed_objects) == 0

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            with custom_client.batch.fixed_size() as batch:
                batch.add_object(collection=col1.name, properties={}, tenant=tenant)
                batch.add_object(collection=col2.name, properties={}, tenant=tenant)
            # only one permission is missing, so one object will fail
            assert len(custom_client.batch.failed_objects) == 1
    admin_client.collections.delete([name + "1", name + "2"])
