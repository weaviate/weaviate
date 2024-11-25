import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_obj_insert(request: SubRequest, admin_client, custom_client):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(name=name)

    admin_client.roles.create(
        name=name,
        permissions=[
            RBAC.permissions.data.create(collection=col.name),
            RBAC.permissions.config.read(collection=col.name),
        ],
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check

    source_no_rights.data.insert({})

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no metadata read
    role = admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.data.create(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=role.name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        source_no_rights.data.insert({})
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=role.name)
    admin_client.roles.delete(role.name)

    # no data create
    role = admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.config.read(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=role.name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        source_no_rights.data.insert({})
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=role.name)
    admin_client.roles.delete(role.name)


def test_obj_replace(request: SubRequest, admin_client, custom_client):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(name=name)

    uuid_to_replace = col.data.insert({})

    admin_client.roles.create(
        name=name,
        permissions=[
            RBAC.permissions.data.update(collection=col.name),
            RBAC.permissions.config.read(collection=col.name),
        ],
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check

    source_no_rights.data.replace(uuid=uuid_to_replace, properties={})

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no metadata read
    admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.data.update(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        source_no_rights.data.replace(uuid=uuid_to_replace, properties={})
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no data create
    admin_client.roles.create(
        name=name, permissions=RBAC.permissions.config.read(collection=col.name)
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        source_no_rights.data.replace(uuid=uuid_to_replace, properties={})
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)
    admin_client.collections.delete(name)


def test_obj_update(request: SubRequest, admin_client, custom_client):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(name=name)

    uuid_to_replace = col.data.insert({})

    admin_client.roles.create(
        name=name,
        permissions=[
            RBAC.permissions.data.update(collection=col.name),
            RBAC.permissions.config.read(collection=col.name),
        ],
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check

    source_no_rights.data.update(uuid=uuid_to_replace, properties={})

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no metadata read
    admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.data.update(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        source_no_rights.data.update(uuid=uuid_to_replace, properties={})
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no data update
    admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.config.read(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    source_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        source_no_rights.data.replace(uuid=uuid_to_replace, properties={})
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)
    admin_client.collections.delete(name)


def test_obj_delete(request: SubRequest, admin_client, custom_client):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(name=name)

    uuid_to_delete = col.data.insert({})

    admin_client.roles.create(
        name=name,
        permissions=[
            RBAC.permissions.data.delete(collection=col.name),
            RBAC.permissions.config.read(collection=col.name),
        ],
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check

    assert len(col) == 1
    col_no_rights.data.delete_by_id(uuid=uuid_to_delete)
    assert len(col) == 0
    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no metadata read
    uuid_to_delete = col.data.insert({})

    admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.data.delete(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check

    assert len(col) == 1
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        col_no_rights.data.delete_by_id(uuid=uuid_to_delete)
    assert e.value.status_code == 403
    assert len(col) == 1

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no data delete
    admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.config.read(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        col_no_rights.data.delete_by_id(uuid=uuid_to_delete)
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)
    admin_client.collections.delete(name)


def test_obj_exists(request: SubRequest, admin_client, custom_client):
    name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(name)
    admin_client.roles.delete(name)
    col = admin_client.collections.create(name=name)

    uuid_to_check = col.data.insert({})

    admin_client.roles.create(
        name=name,
        permissions=[
            RBAC.permissions.data.read(collection=col.name),
            RBAC.permissions.config.read(collection=col.name),
        ],
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check

    assert col_no_rights.data.exists(uuid=uuid_to_check)
    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no metadata read
    admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.data.read(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=name)

    col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check

    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        col_no_rights.data.exists(uuid=uuid_to_check)
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=name)
    admin_client.roles.delete(name)

    # no data read
    both_write = admin_client.roles.create(
        name=name,
        permissions=RBAC.permissions.config.read(collection=col.name),
    )
    admin_client.roles.assign(user="custom-user", roles=both_write.name)

    col_no_rights = custom_client.collections.get(name)  # no network call => no RBAC check
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        col_no_rights.data.exists(uuid=uuid_to_check)
    assert e.value.status_code == 403

    admin_client.roles.revoke(user="custom-user", roles=both_write.name)
    admin_client.roles.delete(both_write.name)
    admin_client.collections.delete(name)
