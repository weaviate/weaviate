import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, generate_missing_lists

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_batch_grpc(request: SubRequest, admin_client):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete([name + "1", name + "2"])
    # create two collections with some objects to test refs
    col1 = admin_client.collections.create(name=name + "1")
    col2 = admin_client.collections.create(name=name + "2")
    admin_client.roles.delete(name)

    batch_permissions = [
        RBAC.permissions.data.create(collection=col1.name),
        RBAC.permissions.data.update(collection=col1.name),
        RBAC.permissions.config.read(collection=col1.name),
        RBAC.permissions.data.create(collection=col2.name),
        RBAC.permissions.data.update(collection=col2.name),
        RBAC.permissions.config.read(collection=col2.name),
    ]
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
    ) as client_no_rights:
        admin_client.roles.create(name=name, permissions=batch_permissions)
        admin_client.roles.assign(user="custom-user", roles=name)

        with client_no_rights.batch.fixed_size() as batch:
            batch.add_object(collection=col1.name, properties={})
            batch.add_object(collection=col2.name, properties={})
        assert len(client_no_rights.batch.failed_objects) == 0
        admin_client.roles.revoke(user="custom-user", roles=name)
        admin_client.roles.delete(name)

    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
    ) as client_no_rights:
        for permissions in generate_missing_lists(batch_permissions):
            admin_client.roles.create(name=name, permissions=permissions)
            admin_client.roles.assign(user="custom-user", roles=name)

            with client_no_rights.batch.fixed_size() as batch:
                batch.add_object(collection=col1.name, properties={})
                batch.add_object(collection=col2.name, properties={})
            # only one permission is missing, so one object will fail
            assert len(client_no_rights.batch.failed_objects) == 1
            admin_client.roles.revoke(user="custom-user", roles=name)
            admin_client.roles.delete(name)
