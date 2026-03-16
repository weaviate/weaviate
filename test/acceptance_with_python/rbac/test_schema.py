import datetime

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.classes.config import Configure, Reconfigure
from weaviate.rbac.models import Permissions
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, RoleWrapperProtocol, generate_missing_permissions

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_rbac_collection_create(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    required_permissions = [
        Permissions.collections(collection=name, read_config=True, create_collection=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        custom_client.collections.create(name=name)
        admin_client.collections.delete(name)

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                custom_client.collections.create(name=name)
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_rbac_collection_create_with_ref(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest, mt: bool
):
    name_target = _sanitize_role_name(request.node.name) + "target"
    name_source = _sanitize_role_name(request.node.name) + "source"
    admin_client.collections.delete([name_target, name_source])
    target = admin_client.collections.create(
        name=name_target, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )

    required_permissions = [
        Permissions.collections(collection=[name_source, target.name], read_config=True),
        Permissions.collections(collection=name_source, create_collection=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        custom_client.collections.create(
            name=name_source,
            multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
            references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        )

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                custom_client.collections.create(
                    name=name_source,
                    multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
                    references=[
                        wvc.config.ReferenceProperty(name="ref", target_collection=target.name)
                    ],
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

    admin_client.collections.delete([name_target, name_source])


def test_rbac_collection_read(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)

    required_permissions = Permissions.collections(collection=name, read_config=True)
    with role_wrapper(admin_client, request, required_permissions):
        col = custom_client.collections.get(name=name)
        assert col.config.get() is not None

    with role_wrapper(admin_client, request, []):
        col = custom_client.collections.get(name=name)
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            col.config.get()
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]

    admin_client.collections.delete(name)


def test_rbac_schema_read(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)

    required_permission = Permissions.collections(collection="*", read_config=True)
    with role_wrapper(admin_client, request, required_permission):
        custom_client.collections.list_all()

    with role_wrapper(admin_client, request, []):
        collections = custom_client.collections.list_all()
        assert len(collections) == 0

    admin_client.collections.delete(name)


def test_rbac_schema_read_filtered_collections(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    base_name = _sanitize_role_name(request.node.name)
    allowed_collection = f"{base_name}_allowed"
    restricted_collection = f"{base_name}_restricted"

    for name in [allowed_collection, restricted_collection]:
        admin_client.collections.delete(name)
        admin_client.collections.create(name=name)

    required_permission = Permissions.collections(collection=allowed_collection, read_config=True)
    with role_wrapper(admin_client, request, required_permission):
        collections = custom_client.collections.list_all()
        collection_names = {name.lower() for name in collections.keys()}
        assert len(collection_names) == 1
        assert allowed_collection.lower() in collection_names
        assert restricted_collection.lower() not in collection_names

    admin_client.collections.delete(allowed_collection)
    admin_client.collections.delete(restricted_collection)


def test_rbac_collection_update(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)

    required_permissions = [
        Permissions.collections(collection=name, read_config=True, update_config=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        col_custom = custom_client.collections.get(name)
        col_custom.config.update(description="test")

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            col_custom = custom_client.collections.get(name)
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                col_custom.config.update(description="test")
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

    admin_client.collections.delete(name)


def test_rbac_collection_update_with_ref(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name_target = _sanitize_role_name(request.node.name) + "target"
    name_source = _sanitize_role_name(request.node.name) + "source"
    admin_client.collections.delete([name_target, name_source])
    admin_client.collections.create(name=name_target)
    admin_client.collections.create(name=name_source)

    required_permissions = [
        Permissions.collections(collection=[name_target, name_source], read_config=True),
        Permissions.collections(collection=name_source, update_config=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        col_custom = custom_client.collections.get(name_source)
        col_custom.config.add_reference(
            wvc.config.ReferenceProperty(name="self1", target_collection=name_target)
        )

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            col_custom = custom_client.collections.get(name_source)
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                col_custom.config.add_reference(
                    wvc.config.ReferenceProperty(name="self2", target_collection=name_target)
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

    admin_client.collections.delete([name_target, name_source])


def test_rbac_collection_delete(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)

    required_permissions = [
        Permissions.collections(collection=name, delete_collection=True, read_config=True),
    ]
    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                custom_client.collections.delete(name)
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]
            assert admin_client.collections.get(name) is not None

    with role_wrapper(admin_client, request, required_permissions):
        custom_client.collections.delete(name)
        assert not admin_client.collections.exists(name)

    admin_client.collections.delete(name)


def test_rbac_collection_create_with_ttl(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)

    required_permissions = [
        Permissions.collections(collection=name, read_config=True, create_collection=True),
        Permissions.data(collection=name, delete=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        custom_client.collections.create(
            name=name,
            properties=[
                wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
            ],
            object_ttl_config=Configure.ObjectTTL.delete_by_date_property(
                property_name="custom_date",
                ttl_offset=datetime.timedelta(minutes=10),
            ),
        )
        admin_client.collections.delete(name)

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                custom_client.collections.create(
                    name=name,
                    properties=[
                        wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
                    ],
                    object_ttl_config=Configure.ObjectTTL.delete_by_date_property(
                        property_name="custom_date",
                        ttl_offset=datetime.timedelta(minutes=10),
                    ),
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]
    admin_client.collections.delete(name)


def test_rbac_update_existing_ttl_no_delete_perm(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    """A user without data delete permission should be able to update a collection
    that already has TTL enabled, as long as they don't change TTL settings."""
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(
        name=name,
        properties=[
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl_config=Configure.ObjectTTL.delete_by_date_property(
            property_name="custom_date",
            ttl_offset=datetime.timedelta(minutes=10),
        ),
    )

    required_permissions = [
        Permissions.collections(collection=name, read_config=True, update_config=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        col_custom = custom_client.collections.get(name)
        col_custom.config.update(description="updated description")

        config = col_custom.config.get()
        assert config.description == "updated description"

    admin_client.collections.delete(name)


def test_rbac_change_ttl_requires_delete_perm(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    """Changing TTL settings (e.g. ttl offset) on a collection requires data delete permission,
    even when TTL stays enabled."""
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(
        name=name,
        properties=[
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl_config=Configure.ObjectTTL.delete_by_date_property(
            property_name="custom_date",
            ttl_offset=datetime.timedelta(minutes=10),
        ),
    )

    # Without delete permission, changing TTL offset should fail
    permissions_without_delete = [
        Permissions.collections(collection=name, read_config=True, update_config=True),
    ]
    with role_wrapper(admin_client, request, permissions_without_delete):
        col_custom = custom_client.collections.get(name)
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            col_custom.config.update(
                object_ttl_config=Reconfigure.ObjectTTL.delete_by_date_property(
                    property_name="custom_date",
                    ttl_offset=datetime.timedelta(minutes=20),
                ),
            )
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]

    # With delete permission, changing TTL offset should succeed
    permissions_with_delete = [
        Permissions.collections(collection=name, read_config=True, update_config=True),
        Permissions.data(collection=name, delete=True),
    ]
    with role_wrapper(admin_client, request, permissions_with_delete):
        col_custom = custom_client.collections.get(name)
        col_custom.config.update(
            object_ttl_config=Reconfigure.ObjectTTL.delete_by_date_property(
                property_name="custom_date",
                ttl_offset=datetime.timedelta(minutes=20),
            ),
        )

    admin_client.collections.delete(name)


def test_rbac_collection_disable_ttl_requires_delete_permission(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    """Disabling TTL on a collection requires data delete permission."""
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(
        name=name,
        properties=[
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl_config=Configure.ObjectTTL.delete_by_date_property(
            property_name="custom_date",
            ttl_offset=datetime.timedelta(minutes=10),
        ),
    )

    # Without delete permission, disabling TTL should fail
    permissions_without_delete = [
        Permissions.collections(collection=name, read_config=True, update_config=True),
    ]
    with role_wrapper(admin_client, request, permissions_without_delete):
        col_custom = custom_client.collections.get(name)
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            col_custom.config.update(object_ttl_config=Reconfigure.ObjectTTL.disable())
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]

    # With delete permission, disabling TTL should succeed
    permissions_with_delete = [
        Permissions.collections(collection=name, read_config=True, update_config=True),
        Permissions.data(collection=name, delete=True),
    ]
    with role_wrapper(admin_client, request, permissions_with_delete):
        col_custom = custom_client.collections.get(name)
        col_custom.config.update(object_ttl_config=Reconfigure.ObjectTTL.disable())

    admin_client.collections.delete(name)


def test_rbac_collection_update_with_ttl(
    admin_client, custom_client, role_wrapper: RoleWrapperProtocol, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(
        name=name,
        properties=[
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
    )

    required_permissions = [
        Permissions.collections(collection=name, read_config=True, update_config=True),
        Permissions.data(collection=name, delete=True),
    ]

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            col_custom = custom_client.collections.get(name)
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                col_custom.config.update(
                    object_ttl_config=Reconfigure.ObjectTTL.delete_by_date_property(
                        property_name="custom_date",
                        ttl_offset=datetime.timedelta(minutes=10),
                    ),
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

    with role_wrapper(admin_client, request, required_permissions):
        col_custom = custom_client.collections.get(name)
        col_custom.config.update(
            object_ttl_config=Reconfigure.ObjectTTL.delete_by_date_property(
                property_name="custom_date",
                ttl_offset=datetime.timedelta(minutes=10),
            ),
        )

    admin_client.collections.delete(name)
