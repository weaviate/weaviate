import pytest
import weaviate
from _pytest.fixtures import SubRequest
from weaviate.rbac.models import Permissions

from .conftest import _sanitize_role_name, role_wrapper, RoleWrapperProtocol

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_rbac_viewer_assign(
    request: SubRequest, admin_client, viewer_client, role_wrapper: RoleWrapperProtocol
):
    name = _sanitize_role_name(request.node.name)

    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)

    # cannot delete with viewer permissions
    with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
        viewer_client.collections.delete(name)

    # with extra role that has those permissions it works
    with role_wrapper(
        admin_client,
        request,
        Permissions.collections(collection=name, delete_collection=True),
        "viewer-user",
    ):
        viewer_client.collections.delete(name)

    admin_client.collections.delete(name)


def test_rbac_with_regexp(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol
):
    name = _sanitize_role_name(request.node.name)
    base = "python_"
    python_name = base + name
    admin_client.collections.delete([name, python_name])
    admin_client.collections.create(name=name)
    admin_client.collections.create(name=python_name)

    with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
        custom_client.collections.delete(python_name)

    # can delete everything starting with "python_" but nothing else
    required_permissions = [
        Permissions.collections(collection="*", read_config=True),
        Permissions.collections(collection=base + "*", delete_collection=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        custom_client.collections.delete(python_name)
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            custom_client.collections.delete(name)
    admin_client.collections.delete([name, python_name])


def test_roles_restrictions_create(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol
):
    name = _sanitize_role_name(request.node.name)

    custom_client.roles.delete(name + "_random_string")
    custom_client.roles.delete(name + "_random_string2")

    required_permissions = [
        Permissions.collections(collection="ABC_", read_config=True),
        Permissions.roles(manage=True, role=name + "_random_*"),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        # same permissions as wrapping role => succeed
        custom_client.roles.create(
            role_name=name + "_random_string",
            permissions=[
                Permissions.collections(collection="ABC_", read_config=True),
                Permissions.roles(read=True, manage=True, role=name + "_random_*"),
            ],
        )
        admin_client.roles.delete(name + "_random_string")

        # fewer permissions as wrapping role => succeed
        custom_client.roles.create(
            role_name=name + "_random_string2",
            permissions=[
                Permissions.roles(read=True, manage=False, role=name + "_random_string"),
            ],
        )
        admin_client.roles.delete(name + "_random_string2")

        # more permissions => fail
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            custom_client.roles.create(
                role_name=name + "_random_string3",
                permissions=[
                    Permissions.collections(collection="ABC_", read_config=True),
                    Permissions.roles(
                        read=True, manage=False, role=name + "*"
                    ),  # name is less restrictive
                ],
            )
        assert e.value.status_code == 403

        # more permissions case 2 => fail
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            custom_client.roles.create(
                role_name=name + "_random_string4",
                permissions=[
                    Permissions.collections(
                        collection="*", read_config=True
                    ),  # collection is less restrictive
                    Permissions.roles(read=True, manage=True, role=name + "_random_*"),
                ],
            )
        assert e.value.status_code == 403

        # more permissions case 3 => fail
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            custom_client.roles.create(
                role_name=name + "_random_string4",
                permissions=[
                    Permissions.collections(collection="ABC_", read_config=True),
                    Permissions.roles(read=True, manage=True, role=name + "_random_*"),
                    Permissions.data(collection="ABC_*", read=True),
                ],
            )
        assert e.value.status_code == 403


def test_roles_restrictions_update(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol
):
    name = _sanitize_role_name(request.node.name)
    update_role_name = name + "_update"
    admin_client.roles.delete(update_role_name)

    required_permissions = [
        Permissions.collections(collection="ABC_*", read_config=True),
        Permissions.roles(manage=True, role=name + "_*"),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        custom_client.roles.create(
            role_name=update_role_name,
            permissions=[],
        )

        # exactly the same permissions as original ones
        custom_client.roles.add_permissions(
            role_name=update_role_name,
            permissions=[
                Permissions.roles(manage=True, role=name + "_random_*"),
            ],
        )

        # fewer the same permissions as original ones
        custom_client.roles.add_permissions(
            role_name=update_role_name,
            permissions=[
                Permissions.collections(collection="ABC_DEF", read_config=True),
            ],
        )

        # wider permissions as original ones => fail
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            custom_client.roles.add_permissions(
                role_name=update_role_name,
                permissions=[
                    Permissions.collections(collection="AB*", read_config=True),
                ],
            )
        assert e.value.status_code == 403

        # different permission type as original ones => fail
        with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
            custom_client.roles.add_permissions(
                role_name=update_role_name,
                permissions=[
                    Permissions.backup(collection="AB*", manage=True),
                ],
            )
        assert e.value.status_code == 403

    role = admin_client.roles.by_name(update_role_name)
    assert len(role.permissions) == 2
    admin_client.roles.delete(update_role_name)
