import uuid

import pytest
import weaviate
import weaviate.classes as wvc
from _pytest.fixtures import SubRequest
from weaviate.collections.classes.data import DataReference
from weaviate.rbac.models import Permissions
from weaviate.rbac.roles import _flatten_permissions

from .conftest import _sanitize_role_name, generate_missing_permissions, RoleWrapperProtocol

pytestmark = pytest.mark.xdist_group(name="rbac")


@pytest.mark.parametrize("mt", [True, False])
def test_rbac_refs(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    col_name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete([col_name + "target", col_name + "source"])
    # create two collections with some objects to test refs
    target = admin_client.collections.create(
        name=col_name + "target",
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    source = admin_client.collections.create(
        name=col_name + "source",
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    if mt:
        target.tenants.create("tenant1")
        source.tenants.create("tenant1")
        target = target.with_tenant("tenant1")
        source = source.with_tenant("tenant1")

    uuid_target1 = target.data.insert({})
    uuid_target2 = target.data.insert({})
    uuid_source = source.data.insert(properties={})
    role_name = _sanitize_role_name(request.node.name)
    admin_client.roles.delete(role_name)

    required_permissions = [
        Permissions.collections(collection=[source.name, target.name], read_config=True),
        Permissions.data(collection=[source.name, target.name], update=True, read=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(
            source.name
        )  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")

        source_no_rights.data.reference_add(
            from_uuid=uuid_source,
            from_property="ref",
            to=uuid_target1,
        )

        source_no_rights.data.reference_replace(
            from_uuid=uuid_source,
            from_property="ref",
            to=uuid_target2,
        )

        source_no_rights.data.reference_delete(
            from_uuid=uuid_source,
            from_property="ref",
            to=uuid_target2,
        )

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                source.name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.reference_add(
                    from_uuid=uuid_source,
                    from_property="ref",
                    to=uuid_target1,
                )
            assert e.value.status_code == 403

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.reference_replace(
                    from_uuid=uuid_source,
                    from_property="ref",
                    to=uuid_target2,
                )
            assert e.value.status_code == 403

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.reference_delete(
                    from_uuid=uuid_source,
                    from_property="ref",
                    to=uuid_target1,
                )
            assert e.value.status_code == 403

    admin_client.collections.delete([target.name, source.name])


@pytest.mark.parametrize("mt", [True, False])
def test_batch_delete_with_filter(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
) -> None:
    col_name = _sanitize_role_name(request.node.name)

    admin_client.collections.delete([col_name + "target", col_name + "source"])
    # create two collections with some objects to test refs
    target = admin_client.collections.create(
        name=col_name + "target",
        properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    source = admin_client.collections.create(
        name=col_name + "source",
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    if mt:
        target.tenants.create("tenant1")
        source.tenants.create("tenant1")
        target = target.with_tenant("tenant1")
        source = source.with_tenant("tenant1")

    uuid_target1 = target.data.insert({})

    role_name = _sanitize_role_name(request.node.name)
    admin_client.roles.delete(role_name)

    uuid_source = source.data.insert(properties={}, references={"ref": uuid_target1})
    source.data.reference_add(
        from_uuid=uuid_source,
        from_property="ref",
        to=uuid_target1,
    )

    required_permissions = [
        Permissions.data(collection=source.name, delete=True, read=True),
        Permissions.data(collection=target.name, read=True),
    ]

    # failing permissions first, so the object isn't actually deleted
    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            assert (
                len(source) == 1
            )  # uses aggregate in background, cannot do that with restricted user

            source_no_rights = custom_client.collections.get(
                source.name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")

            # deletion does not work
            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.delete_many(
                    where=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
                )
            assert "forbidden" in e.value.args[0]
            assert (
                len(source) == 1
            )  # uses aggregate in background, cannot do that with restricted user

    with role_wrapper(admin_client, request, required_permissions):
        assert len(source) == 1  # uses aggregate in background, cannot do that with restricted user

        source_no_rights = custom_client.collections.get(
            source.name
        )  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")

        ret = source_no_rights.data.delete_many(
            where=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
        )
        assert ret.successful == 1
        assert len(source) == 0  # uses aggregate in background, cannot do that with restricted user

    admin_client.collections.delete([target.name, source.name])


@pytest.mark.parametrize("mt", [True, False])
def test_search_with_filter_and_return(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
) -> None:
    col_name = _sanitize_role_name(request.node.name)

    admin_client.collections.delete([col_name + "target", col_name + "source"])
    # create two collections with some objects to test refs
    target = admin_client.collections.create(
        name=col_name + "target",
        properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    source = admin_client.collections.create(
        name=col_name + "source",
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    if mt:
        target.tenants.create("tenant1")
        source.tenants.create("tenant1")
        target = target.with_tenant("tenant1")
        source = source.with_tenant("tenant1")

    uuid_target1 = target.data.insert({"prop": "word"})
    source.data.insert(properties={}, references={"ref": uuid_target1})

    role_name = _sanitize_role_name(request.node.name)
    admin_client.roles.delete(role_name)

    required_permissions = [
        Permissions.data(collection=[source.name], read=True),
        Permissions.data(collection=[target.name], read=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(
            source.name
        )  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")

        ret_filter = source_no_rights.query.fetch_objects(
            filters=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1),
        )
        assert len(ret_filter.objects) == 1

        ret_return = source_no_rights.query.fetch_objects(
            return_references=wvc.query.QueryReference(
                link_on="ref",
                return_properties=["prop"],
            ),
        )
        assert len(ret_return.objects[0].references["ref"].objects) == 1

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                source.name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.query.fetch_objects(
                    filters=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
                )
            assert "forbidden" in e.value.args[0]

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.query.fetch_objects(
                    return_references=wvc.query.QueryReference(
                        link_on="ref",
                        return_properties=["prop"],
                    ),
                )
            assert "forbidden" in e.value.args[0]

    admin_client.collections.delete([target.name, source.name])


@pytest.mark.parametrize("mt", [True, False])
def test_batch_ref(
    request: SubRequest, admin_client, custom_client, role_wrapper: RoleWrapperProtocol, mt: bool
):
    col_name = _sanitize_role_name(request.node.name)
    admin_client.collections.delete(
        [col_name + "target1", col_name + "target2", col_name + "source"]
    )
    # create two collections with some objects to test refs
    target1 = admin_client.collections.create(
        name=col_name + "target1",
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    target2 = admin_client.collections.create(
        name=col_name + "target2",
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    source = admin_client.collections.create(
        name=col_name + "source",
        references=[
            wvc.config.ReferenceProperty(name="ref1", target_collection=target1.name),
            wvc.config.ReferenceProperty(name="ref2", target_collection=target2.name),
        ],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=mt),
    )
    if mt:
        target1.tenants.create("tenant1")
        target2.tenants.create("tenant1")
        source.tenants.create("tenant1")
        target1 = target1.with_tenant("tenant1")
        target2 = target2.with_tenant("tenant1")
        source = source.with_tenant("tenant1")

    source.config.add_reference(
        wvc.config.ReferenceProperty(name="self", target_collection=source.name)
    )

    uuid_target1 = target1.data.insert({})
    uuid_target2 = target2.data.insert({})
    uuid_source = source.data.insert(properties={})
    role_name = _sanitize_role_name(request.node.name)
    admin_client.roles.delete(role_name)

    # self reference
    required_permissions = [
        Permissions.collections(collection=source.name, read_config=True),
        Permissions.data(collection=source.name, read=True, update=True),
    ]
    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(
            source.name
        )  # no network call => no RBAC check
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")

        ret = source_no_rights.data.reference_add_many(
            [DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source)]
        )
        assert len(ret.errors) == 0

    for permission in generate_missing_permissions(required_permissions):
        with role_wrapper(admin_client, request, permission):
            source_no_rights = custom_client.collections.get(
                source.name
            )  # no network call => no RBAC check
            if mt:
                source_no_rights = source_no_rights.with_tenant("tenant1")

            with pytest.raises(weaviate.exceptions.InsufficientPermissionsError) as e:
                source_no_rights.data.reference_add_many(
                    [
                        DataReference(
                            from_property="self", from_uuid=uuid_source, to_uuid=uuid_source
                        )
                    ]
                )
            assert e.value.status_code == 403

    # ref to one target
    required_permissions = [
        Permissions.collections(collection=source.name, read_config=True),
        Permissions.data(collection=[source.name, target1.name], read=True),
        Permissions.data(collection=source.name, update=True),
        Permissions.collections(collection=target1.name, read_config=True),
    ]

    with role_wrapper(admin_client, request, required_permissions):
        source_no_rights = custom_client.collections.get(source.name)
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")

        ret = source_no_rights.data.reference_add_many(
            [
                DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                DataReference(from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1),
            ]
        )
        assert len(ret.errors) == 0

    # without read permission for target that one reference fails
    # no rights to read target class
    with role_wrapper(admin_client, request, required_permissions[:-1]):
        source_no_rights = custom_client.collections.get(source.name)
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        ret = source_no_rights.data.reference_add_many(
            [
                DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                DataReference(from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1),
            ]
        )

        assert len(ret.errors) == 1
        assert "forbidden" in ret.errors[1].message

    # ref to two targets
    ref2_required_permissions = [
        Permissions.collections(collection=[source.name, target1.name], read_config=True),
        Permissions.data(collection=source.name, update=True),
        Permissions.data(collection=[source.name, target1.name, target2.name], read=True),
        Permissions.collections(collection=target2.name, read_config=True),
    ]

    with role_wrapper(admin_client, request, ref2_required_permissions):
        source_no_rights = custom_client.collections.get(source.name)
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        ret = source_no_rights.data.reference_add_many(
            [
                DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                DataReference(from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1),
                DataReference(from_property="ref2", from_uuid=uuid_source, to_uuid=uuid_target2),
            ]
        )
        assert len(ret.errors) == 0

    # without read permission for target collection config, references TO that collection fail
    with role_wrapper(admin_client, request, ref2_required_permissions[:-1]):
        source_no_rights = custom_client.collections.get(source.name)
        if mt:
            source_no_rights = source_no_rights.with_tenant("tenant1")
        ret = source_no_rights.data.reference_add_many(
            [
                DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                DataReference(from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1),
                DataReference(from_property="ref2", from_uuid=uuid_source, to_uuid=uuid_target2),
            ]
        )

        # only target where we miss permissions is failing
        assert len(ret.errors) == 1
        assert "forbidden" in ret.errors[2].message

    admin_client.collections.delete(
        [col_name + "target1", col_name + "target2", col_name + "source"]
    )
