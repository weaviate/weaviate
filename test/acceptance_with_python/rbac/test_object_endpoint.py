import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import Permissions
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, RoleWrapperProtocol, generate_missing_permissions

pytestmark = pytest.mark.xdist_group(name="rbac")


@pytest.mark.parametrize("mt", [True, False])
def test_obj_insert(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(
        name=name, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    if mt:
        col.tenants.create("tenant1")

    required_permissions = [
        Permissions.data(collection=col.name, create=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        source_no_rights.data.insert({})

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.insert({})
            assert e.value.status_code == 403
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_obj_insert_ref(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete([name + "source", name + "target"])
    admin_client.roles.delete(name)
    target = admin_client.collections.create(
        name=name + "target", multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    source = admin_client.collections.create(
        name=name + "source",
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
    )

    if mt:
        source.tenants.create("tenant1")
        target.tenants.create("tenant1")
        target = target.with_tenant("tenant1")

    uuid_target = target.data.insert({})

    required_permissions = [
        Permissions.data(collection=source.name, create=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(
            source.name
        )  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        source_no_rights.data.insert(properties={}, references={"ref": uuid_target})

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                source.name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.insert(properties={}, references={"ref": uuid_target})
            assert e.value.status_code == 403
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_obj_replace(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(
        name=name, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    if mt:
        col.tenants.create("tenant1")
        col = col.with_tenant("tenant1")

    uuid_to_replace = col.data.insert({})

    required_permissions = [
        Permissions.data(collection=col.name, update=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        source_no_rights.data.replace(uuid=uuid_to_replace, properties={})

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.replace(uuid=uuid_to_replace, properties={})
            assert e.value.status_code == 403
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_obj_replace_ref(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete([name + "source", name + "target"])
    admin_client.roles.delete(name)
    target = admin_client.collections.create(
        name=name + "target", multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    source = admin_client.collections.create(
        name=name + "source",
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
    )

    if mt:
        source.tenants.create("tenant1")
        target.tenants.create("tenant1")
        source = source.with_tenant("tenant1")
        target = target.with_tenant("tenant1")

    uuid_target = target.data.insert({})
    uuid_to_replace = source.data.insert({})

    required_permissions = [
        Permissions.data(collection=source.name, update=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(
            source.name
        )  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        source_no_rights.data.replace(
            uuid=uuid_to_replace, properties={}, references={"ref": uuid_target}
        )

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                source.name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.replace(
                    uuid=uuid_to_replace, properties={}, references={"ref": uuid_target}
                )
            assert e.value.status_code == 403
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_obj_update(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(
        name=name, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    if mt:
        col.tenants.create("tenant1")
        col = col.with_tenant("tenant1")

    uuid_to_replace = col.data.insert({})

    required_permissions = [
        Permissions.data(collection=col.name, update=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        source_no_rights.data.update(uuid=uuid_to_replace, properties={})

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.update(uuid=uuid_to_replace, properties={})
            assert e.value.status_code == 403
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_obj_update_ref(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete([name + "source", name + "target"])
    admin_client.roles.delete(name)
    target = admin_client.collections.create(
        name=name + "target", multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    source = admin_client.collections.create(
        name=name + "source",
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
    )

    if mt:
        source.tenants.create("tenant1")
        target.tenants.create("tenant1")
        source = source.with_tenant("tenant1")
        target = target.with_tenant("tenant1")

    uuid_target = target.data.insert({})
    uuid_to_replace = source.data.insert({})

    required_permissions = [
        Permissions.data(collection=source.name, update=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(
            source.name
        )  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        source_no_rights.data.update(
            uuid=uuid_to_replace, properties={}, references={"ref": uuid_target}
        )

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                source.name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.update(
                    uuid=uuid_to_replace, properties={}, references={"ref": uuid_target}
                )
            assert e.value.status_code == 403
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_obj_delete(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(
        name=name, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    if mt:
        col.tenants.create("tenant1")
        col = col.with_tenant("tenant1")

    uuid_to_delete = col.data.insert({})

    required_permissions = [
        Permissions.data(collection=col.name, delete=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
        if mt:
            col_no_rights = col_no_rights.with_tenant("tenant1")
        assert len(col) == 1
        col_no_rights.data.delete_by_id(uuid=uuid_to_delete)
        assert len(col) == 0

    uuid_to_delete = col.data.insert({})
    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
            if mt:
                col_no_rights = col_no_rights.with_tenant("tenant1")

            assert len(col) == 1
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                col_no_rights.data.delete_by_id(uuid=uuid_to_delete)
            assert e.value.status_code == 403
            assert len(col) == 1
    admin_client.collections.delete(name)


@pytest.mark.parametrize("mt", [True, False])
def test_obj_exists(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(
        name=name, multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt)
    )
    if mt:
        col.tenants.create("tenant1")
        col = col.with_tenant("tenant1")

    uuid_to_check = col.data.insert({})

    required_permissions = [
        Permissions.data(collection=col.name, read=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
        if mt:
            col_no_rights = col_no_rights.with_tenant("tenant1")
        assert col_no_rights.data.exists(uuid=uuid_to_check)

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):

            col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
            if mt:
                col_no_rights = col_no_rights.with_tenant("tenant1")

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                col_no_rights.data.exists(uuid=uuid_to_check)
            assert e.value.status_code == 403
    admin_client.collections.delete(name)
