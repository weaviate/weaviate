import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name

pytestmark = pytest.mark.xdist_group(name="rbac")


@pytest.fixture
def test_collection(request: SubRequest, admin_client):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)

    yield name

    admin_client.collections.delete(name)


@pytest.fixture
def test_two_collections(request: SubRequest, admin_client):
    name_1 = _sanitize_role_name(request.node.name) + "col1"
    name_2 = _sanitize_role_name(request.node.name) + "col2"
    admin_client.collections.delete(name_1)
    admin_client.collections.delete(name_2)

    yield name_1, name_2

    admin_client.collections.delete(name_1)
    admin_client.collections.delete(name_2)


@pytest.mark.parametrize(
    "permission_type,should_succeed",
    [
        (RBAC.permissions.collections.create(), True),
        (RBAC.permissions.collections.read(), False),
        (RBAC.permissions.collections.update(), False),
        (RBAC.permissions.collections.delete(), False),
        (
            [
                RBAC.permissions.collections.read(),
                RBAC.permissions.collections.update(),
                RBAC.permissions.collections.delete(),
                RBAC.permissions.collections.objects.create(collection="*"),
                RBAC.permissions.collections.objects.read(collection="*"),
                RBAC.permissions.collections.objects.update(collection="*"),
                RBAC.permissions.collections.objects.delete(collection="*"),
            ],
            False,
        ),
        (
            [
                RBAC.permissions.roles.manage(),
                RBAC.permissions.roles.read(),
            ],
            False,
        ),
        (RBAC.permissions.cluster.manage(), False),
    ],
)
# Test collection create permissions for all collections *.
def test_rbac_collection_create_permissions(
    admin_client, custom_client, test_collection, cleanup_role, permission_type, should_succeed
):
    name_role = cleanup_role
    # Setup role
    admin_client.roles.create(
        name=name_role,
        permissions=permission_type,
    )
    admin_client.roles.assign(user="custom-user", roles=name_role)

    # Test
    if should_succeed:
        col = custom_client.collections.create(
            name=test_collection,
            properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
        )
        assert col is not None
    else:
        with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
            custom_client.collections.create(
                name=test_collection,
                properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
            )
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]

    admin_client.roles.revoke(user="custom-user", roles=name_role)
    admin_client.roles.delete(name_role)
    # Check that after revoking the role, the operation is forbidden
    if should_succeed:
        with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
            custom_client.collections.create(
                name=test_collection,
                properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
            )
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]


@pytest.fixture
def make_permissions_for_collection_create():
    def _make_permissions(col1: str, col2: str):
        return [
            (RBAC.permissions.collections.create(collection=col1), True, False),
            (RBAC.permissions.collections.read(collection=col1), False, False),
            (
                [
                    RBAC.permissions.collections.update(collection=col1),
                    RBAC.permissions.collections.create(collection=col2),
                ],
                False,
                True,
            ),
            (
                [
                    RBAC.permissions.collections.delete(collection=col1),
                    RBAC.permissions.collections.delete(collection=col2),
                ],
                False,
                False,
            ),
            (RBAC.permissions.collections.objects.create(collection=col1), False, False),
            (RBAC.permissions.collections.objects.read(collection=col1), False, False),
            (RBAC.permissions.collections.objects.update(collection=col1), False, False),
            (RBAC.permissions.collections.objects.delete(collection=col1), False, False),
        ]

    return _make_permissions


# Test collection create permissions for a specific collection.
def test_rbac_collection_create_specific_collection(
    admin_client,
    test_two_collections,
    custom_client,
    cleanup_role,
    make_permissions_for_collection_create,
):
    name_role = cleanup_role
    name_collection_1, name_collection_2 = test_two_collections

    # as we can't parametrize the fixture because the name of the collection is unknown at that
    # point, we generate a dictionary of parameters and iterate over it.
    for (
        permission_type,
        should_succeed_collection_1,
        should_succeed_collection_2,
    ) in make_permissions_for_collection_create(
        name_collection_1.capitalize(), name_collection_2.capitalize()
    ):
        admin_client.roles.create(
            name=name_role,
            permissions=permission_type,
        )
        admin_client.roles.assign(user="custom-user", roles=name_role)

        if should_succeed_collection_1:
            col = custom_client.collections.create(
                name=name_collection_1,
                properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
            )
            assert col is not None
        else:
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                custom_client.collections.create(
                    name=name_collection_1,
                    properties=[
                        wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)
                    ],
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

        if should_succeed_collection_2:
            col = custom_client.collections.create(
                name=name_collection_2,
                properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
            )
            assert col is not None
        else:
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                custom_client.collections.create(
                    name=name_collection_2,
                    properties=[
                        wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)
                    ],
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]


@pytest.mark.parametrize(
    "permission_type,should_succeed",
    [
        (
            [
                RBAC.permissions.collections.create(),
                RBAC.permissions.collections.read(),  # batch needs read
                RBAC.permissions.collections.objects.create(collection="*"),
            ],
            True,
        ),
        (
            [
                RBAC.permissions.collections.read(),  # batch needs read
                RBAC.permissions.collections.objects.create(collection="*"),
            ],
            False,
        ),
    ],
)
def test_rbac_collection_create_autoschema(
    admin_client, custom_client, test_collection, cleanup_role, permission_type, should_succeed
):
    name_role = cleanup_role
    admin_client.roles.create(
        name=name_role,
        permissions=permission_type,
    )
    admin_client.roles.assign(user="custom-user", roles=name_role)

    if should_succeed:
        with custom_client.batch.dynamic() as batch:
            batch.add_object(
                collection=test_collection,
                properties={"prop": "test"},
            )
        assert custom_client.batch.failed_objects == []
    else:
        with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
            with custom_client.batch.dynamic() as batch:
                batch.add_object(
                    collection=test_collection,
                    properties={"prop": "test"},
                )
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]


@pytest.mark.parametrize(
    "operation,permission,should_succeed",
    [
        ("exists", RBAC.permissions.collections.read(), True),
        ("config", RBAC.permissions.collections.read(), True),
        ("shards", RBAC.permissions.collections.read(), True),
        ("list_all", RBAC.permissions.collections.read(), True),
        ("query", RBAC.permissions.collections.read(), False),
        (
            "exists",
            [
                RBAC.permissions.collections.update(),
                RBAC.permissions.collections.delete(),
                RBAC.permissions.collections.create(),
            ],
            False,
        ),
        (
            "config",
            [
                RBAC.permissions.collections.update(),
                RBAC.permissions.collections.delete(),
                RBAC.permissions.collections.create(),
            ],
            False,
        ),
        (
            "shards",
            [
                RBAC.permissions.collections.update(),
                RBAC.permissions.collections.delete(),
                RBAC.permissions.collections.create(),
            ],
            False,
        ),
        (
            "list_all",
            [
                RBAC.permissions.collections.update(),
                RBAC.permissions.collections.delete(),
                RBAC.permissions.collections.create(),
            ],
            False,
        ),
    ],
)
def test_rbac_collection_read_permissions(
    admin_client,
    custom_client,
    test_collection,
    cleanup_role,
    operation,
    permission,
    should_succeed,
):
    # Setup collection
    col = admin_client.collections.create(
        name=test_collection,
        properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
    )
    uuid = col.data.insert(
        properties={"prop": "test"},
    )
    # Setup role
    admin_client.roles.create(
        name=cleanup_role,
        permissions=permission,
    )
    admin_client.roles.assign(user="custom-user", roles=cleanup_role)

    # Read operations possible
    operations = {
        "exists": lambda: custom_client.collections.exists(test_collection),
        "config": lambda: custom_client.collections.get(test_collection).config.get(),
        "shards": lambda: custom_client.collections.get(test_collection).config.get_shards(),
        "list_all": lambda: custom_client.collections.list_all(),
        "query": lambda: custom_client.collections.get(test_collection).query.fetch_object_by_id(
            uuid=uuid
        ),
    }

    if should_succeed:
        result = operations[operation]()
        assert result is not None
    else:
        if operation == "query":
            with pytest.raises(weaviate.exceptions.WeaviateQueryError) as e:
                result = operations[operation]()
            assert "forbidden" in e.value.message
        else:
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                result = operations[operation]()
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

    admin_client.roles.revoke(user="custom-user", roles=cleanup_role)
    admin_client.roles.delete(cleanup_role)
    # Check that after revoking the role, the operation is forbidden
    if should_succeed:
        with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
            result = operations[operation]()
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]


@pytest.fixture
def make_permissions_for_collection_read():
    def _make_permissions(col1: str, col2: str):
        return [
            (RBAC.permissions.collections.read(collection=col1), True, False),
            (RBAC.permissions.collections.create(collection=col1), False, False),
            (
                [
                    RBAC.permissions.collections.update(collection=col1),
                    RBAC.permissions.collections.read(collection=col2),
                ],
                False,
                True,
            ),
            (
                [
                    RBAC.permissions.collections.delete(collection=col1),
                    RBAC.permissions.collections.delete(collection=col2),
                ],
                False,
                False,
            ),
            (RBAC.permissions.collections.objects.create(collection=col1), False, False),
            (RBAC.permissions.collections.objects.read(collection=col1), False, False),
            (RBAC.permissions.collections.objects.update(collection=col1), False, False),
            (RBAC.permissions.collections.objects.delete(collection=col1), False, False),
        ]

    return _make_permissions


# Test collection read permissions for a specific collection.
# As we can't parametrize the fixture because the name of the collection is unknown at that
# point, we generate a dictionary of parameters and iterate over it.
def test_rbac_collection_read_specific_collection(
    admin_client,
    custom_client,
    test_two_collections,
    cleanup_role,
    make_permissions_for_collection_read,
):
    name_role = cleanup_role
    name_collection_1, name_collection_2 = test_two_collections

    col_1 = admin_client.collections.create(
        name=name_collection_1,
        properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
    )
    uuid_1 = col_1.data.insert(
        properties={"prop": "test"},
    )

    col_2 = admin_client.collections.create(
        name=name_collection_2,
        properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
    )
    uuid_2 = col_2.data.insert(
        properties={"prop": "test"},
    )

    operations_collection_1 = {
        "exists": lambda: custom_client.collections.exists(col_1.name),
        "config": lambda: custom_client.collections.get(col_1.name).config.get(),
        "shards": lambda: custom_client.collections.get(col_1.name).config.get_shards(),
    }

    operations_collection_2 = {
        "exists": lambda: custom_client.collections.exists(col_2.name),
        "config": lambda: custom_client.collections.get(col_2.name).config.get(),
        "shards": lambda: custom_client.collections.get(col_2.name).config.get_shards(),
    }

    for (
        permission_type,
        should_succeed_collection_1,
        should_succeed_collection_2,
    ) in make_permissions_for_collection_read(col_1.name, col_2.name):
        admin_client.roles.create(
            name=name_role,
            permissions=permission_type,
        )
        admin_client.roles.assign(user="custom-user", roles=name_role)

        # It shouldn't be possible to list all collections in any case
        with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
            custom_client.collections.list_all()
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]

        if should_succeed_collection_1:
            for operation in operations_collection_1:
                result = operations_collection_1[operation]()
                assert result is not None
        else:
            for operation in operations_collection_1:
                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    result = operations_collection_1[operation]()

        if should_succeed_collection_2:
            for operation in operations_collection_2:
                result = operations_collection_2[operation]()
                assert result is not None
        else:
            for operation in operations_collection_2:
                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    result = operations_collection_2[operation]()
