import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, Role_Wrapper_Type, generate_missing_lists

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_rbac_collection_create(
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
                custom_client.collections.create(name=name)
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]
    admin_client.collections.delete(name)


def test_rbac_collection_read(
    admin_client, custom_client, role_wrapper: Role_Wrapper_Type, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)

    with role_wrapper(admin_client, request, RBAC.permissions.config.read(collection=name)):
        col = custom_client.collections.get(name=name)
        assert col.config.get() is not None

    with role_wrapper(admin_client, request, []):
        col = custom_client.collections.get(name=name)
        with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
            col.config.get()
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]
    admin_client.collections.delete(name)


def test_rbac_schema_read(
    admin_client, custom_client, role_wrapper: Role_Wrapper_Type, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)

    with role_wrapper(admin_client, request, RBAC.permissions.config.read()):
        custom_client.collections.list_all()

    with role_wrapper(admin_client, request, []):
        with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
            custom_client.collections.list_all()
        assert e.value.status_code == 403
        assert "forbidden" in e.value.args[0]
    admin_client.collections.delete(name)


def test_rbac_collection_update(
    admin_client, custom_client, role_wrapper: Role_Wrapper_Type, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)
    permissions = [
        RBAC.permissions.config.read(collection=name),
        RBAC.permissions.config.update(collection=name),
    ]
    with role_wrapper(admin_client, request, permissions):
        col_custom = custom_client.collections.get(name)
        col_custom.config.update(description="test")

    for permission in generate_missing_lists(permissions):
        with role_wrapper(admin_client, request, permission):
            col_custom = custom_client.collections.get(name)
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                col_custom.config.update(description="test")
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]
    admin_client.collections.delete(name)


def test_rbac_collection_delete(
    admin_client, custom_client, role_wrapper: Role_Wrapper_Type, request: SubRequest
):
    name = _sanitize_role_name(request.node.name) + "col"
    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)
    permissions = [
        RBAC.permissions.config.read(collection=name),
        RBAC.permissions.config.delete(collection=name),
    ]

    for permission in generate_missing_lists(permissions):
        with role_wrapper(admin_client, request, permission):
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                custom_client.collections.delete(name)
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]
            assert admin_client.collections.get(name) is not None

    with role_wrapper(admin_client, request, permissions):
        custom_client.collections.delete(name)
        assert not admin_client.collections.exists(name)

    admin_client.collections.delete(name)
