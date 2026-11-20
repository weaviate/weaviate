import pytest
import weaviate
from _pytest.fixtures import SubRequest
from weaviate.rbac.models import Permissions

from .conftest import _sanitize_role_name, role_wrapper, RoleWrapperProtocol

pytestmark = pytest.mark.xdist_group(name="rbac")


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


def test_roles_with_backups_grants_parse(request: SubRequest, admin_client):
    # The pinned released client sends no X-Weaviate-Accept-Backup-Principals
    # header and fails to parse a backups permission without a collection.
    # The server derives users and roles backup grants for the role created
    # here, and the built-in admin role displays them with namespaces off.
    name = _sanitize_role_name(request.node.name)
    admin_client.roles.delete(name)
    try:
        admin_client.roles.create(
            role_name=name,
            permissions=Permissions.backup(collection="BackupParse", manage=True),
        )
        role = admin_client.roles.get(name)
        assert role is not None
        assert [p.collection for p in role.backups_permissions] == ["BackupParse"]

        assert admin_client.roles.get("admin") is not None
    finally:
        admin_client.roles.delete(name)
