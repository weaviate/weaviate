import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.exceptions import UnexpectedStatusCodeError
from weaviate.rbac.models import Permissions
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_db_user_create_collection(request: SubRequest, admin_client):
    admin_client.users.db.delete(user_id="test-user")
    api_key = admin_client.users.db.create(user_id="test-user")

    admin_client.users.db.assign_roles(user_id="test-user", role_names="admin")

    collection_name = _sanitize_role_name(request.node.name) + "col"

    # normal collection
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key(api_key)
    ) as custom_client:
        admin_client.collections.delete(collection_name)

        collection = custom_client.collections.create(
            collection_name,
            properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        )
        uuid1 = collection.data.insert({"name": "testing"})

        obj = collection.query.fetch_object_by_id(uuid1)
        assert obj.properties["name"] == "testing"

        assert collection.data.delete_by_id(uuid1)
        assert collection.query.fetch_object_by_id(uuid1) is None

        custom_client.collections.delete(collection_name)
        assert not custom_client.collections.exists(collection_name)
    admin_client.users.db.delete(user_id="test-user")


def test_db_user_multi_tenant(request: SubRequest, admin_client):
    admin_client.users.db.delete(user_id="test-user")
    api_key = admin_client.users.db.create(user_id="test-user")

    admin_client.users.db.assign_roles(user_id="test-user", role_names="admin")

    collection_name = _sanitize_role_name(request.node.name) + "col"
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key(api_key)
    ) as custom_client:
        admin_client.collections.delete(collection_name)

        collection = custom_client.collections.create(
            collection_name,
            properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
            multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=True),
        )

        collection.tenants.create(["tenant1", "tenant2", "tenant3"])
        collection = collection.with_tenant("tenant1")

        uuid1 = collection.data.insert({"name": "testing"})

        obj = collection.query.fetch_object_by_id(uuid1)
        assert obj.properties["name"] == "testing"

        assert collection.data.delete_by_id(uuid1)
        assert collection.query.fetch_object_by_id(uuid1) is None

        # update tenant status to inactive and fail
        collection.tenants.update(
            wvc.tenants.TenantUpdate(
                name="tenant2", activity_status=wvc.tenants.TenantUpdateActivityStatus.INACTIVE
            )
        )
        collection = collection.with_tenant("tenant2")
        with pytest.raises(UnexpectedStatusCodeError):
            collection.data.insert({"name": "testing"})

        custom_client.collections.delete(collection_name)
        assert not custom_client.collections.exists(collection_name)
    admin_client.users.db.delete(user_id="test-user")


def test_db_user_role_and_users(request: SubRequest, admin_client):
    admin_client.users.db.delete(user_id="test-user")
    admin_client.users.db.delete(user_id="second-user")
    api_key = admin_client.users.db.create(user_id="test-user")

    admin_client.users.db.assign_roles(user_id="test-user", role_names="admin")

    collection_name = _sanitize_role_name(request.node.name) + "col"
    role_name = _sanitize_role_name(request.node.name) + "role"
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key(api_key)
    ) as custom_client:
        admin_client.collections.delete(collection_name)
        admin_client.roles.delete(role_name)
        custom_client.roles.create(
            role_name=role_name,
            permissions=[
                Permissions.data(collection=collection_name, read=True, create=True),
                Permissions.collections(collection=collection_name, create_collection=True),
            ],
        )

        second_api_key = custom_client.users.db.create(user_id="second-user")
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key(second_api_key)
        ) as second_client:
            # cannot create collection without permissions
            with pytest.raises(UnexpectedStatusCodeError):
                second_client.collections.create(collection_name)

            custom_client.users.db.assign_roles(user_id="second-user", role_names=role_name)
            second_client.collections.create(collection_name)
    admin_client.users.db.delete(user_id="test-user")
    admin_client.users.db.delete(user_id="second-user")
    admin_client.collections.delete(collection_name)


def test_db_user_batch_import(request: SubRequest, admin_client):
    admin_client.users.db.delete(user_id="test-user")
    api_key = admin_client.users.db.create(user_id="test-user")

    admin_client.users.db.assign_roles(user_id="test-user", role_names="admin")

    collection_name = _sanitize_role_name(request.node.name) + "col"
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key(api_key)
    ) as custom_client:
        admin_client.collections.delete(collection_name)
        collection = custom_client.collections.create(
            collection_name,
            properties=[
                wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
                wvc.config.Property(name="counter", data_type=wvc.config.DataType.INT),
            ],
        )
        num_objects = 100
        ret = collection.data.insert_many(
            [{"name": "test" + str(i), "counter": i} for i in range(num_objects)]
        )
        assert len(collection) == num_objects
        uuid_to_check = ret.uuids[25]
        obj = collection.query.fetch_object_by_id(uuid_to_check)
        assert obj.properties["name"] == "test" + str(25)

        assert collection.data.delete_by_id(uuid_to_check)
        assert collection.query.fetch_object_by_id(uuid_to_check) is None
        assert len(collection) == num_objects - 1

        custom_client.collections.delete(collection_name)
        assert not custom_client.collections.exists(collection_name)
    admin_client.users.db.delete(user_id="test-user")
