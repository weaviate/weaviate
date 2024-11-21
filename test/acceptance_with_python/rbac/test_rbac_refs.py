import uuid

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.collections.classes.data import DataReference
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name, generate_missing_lists


def test_rbac_refs(request: SubRequest):
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        col_name = _sanitize_role_name(request.node.name)
        client.collections.delete([col_name + "target", col_name + "source"])
        # create two collections with some objects to test refs
        target = client.collections.create(name=col_name + "target")
        source = client.collections.create(
            name=col_name + "source",
            references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        )
        uuid_target1 = target.data.insert({})
        uuid_target2 = target.data.insert({})
        uuid_source = source.data.insert(properties={})
        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)

        needed_permissions = [
            RBAC.permissions.collections.read(collection=target.name),
            RBAC.permissions.collections.read(collection=source.name),
            RBAC.permissions.collections.objects.update(collection=source.name),
        ]
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            both_write = client.roles.create(
                name=role_name,
                permissions=needed_permissions,
            )
            client.roles.assign(user="custom-user", roles=both_write.name)

            source_no_rights = client_no_rights.collections.get(
                source.name
            )  # no network call => no RBAC check
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

            client.roles.revoke(user="custom-user", roles=both_write.name)
            client.roles.delete(both_write.name)

        for permissions in generate_missing_lists(needed_permissions):
            with weaviate.connect_to_local(
                port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
            ) as client_no_rights:
                role = client.roles.create(name=role_name, permissions=permissions)
                client.roles.assign(user="custom-user", roles=role.name)

                source_no_rights = client_no_rights.collections.get(
                    source.name
                )  # no network call => no RBAC check

                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    source_no_rights.data.reference_add(
                        from_uuid=uuid_source,
                        from_property="ref",
                        to=uuid_target1,
                    )
                assert e.value.status_code == 403

                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    source_no_rights.data.reference_replace(
                        from_uuid=uuid_source,
                        from_property="ref",
                        to=uuid_target2,
                    )
                assert e.value.status_code == 403

                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    source_no_rights.data.reference_delete(
                        from_uuid=uuid_source,
                        from_property="ref",
                        to=uuid_target1,
                    )
                assert e.value.status_code == 403

                client.roles.revoke(user="custom-user", roles=role.name)
                client.roles.delete(role.name)

        client.collections.delete([target.name, source.name])


def test_batch_delete_with_filter(request: SubRequest) -> None:
    col_name = _sanitize_role_name(request.node.name)

    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        client.collections.delete([col_name + "target", col_name + "source"])
        # create two collections with some objects to test refs
        target = client.collections.create(
            name=col_name + "target",
            properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
        )
        source = client.collections.create(
            name=col_name + "source",
            references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        )
        uuid_target1 = target.data.insert({})

        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)

        # read+delete for both
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            uuid_source = source.data.insert(properties={}, references={"ref": uuid_target1})

            source.data.reference_add(
                from_uuid=uuid_source,
                from_property="ref",
                to=uuid_target1,
            )
            client.roles.create(
                name=role_name,
                permissions=[
                    RBAC.permissions.collections.read(collection=target.name),
                    RBAC.permissions.collections.read(collection=source.name),
                    RBAC.permissions.collections.objects.delete(collection=target.name),
                    RBAC.permissions.collections.objects.delete(collection=source.name),
                    RBAC.permissions.collections.objects.read(
                        collection=target.name,
                    ),
                    RBAC.permissions.collections.objects.read(
                        collection=source.name,
                    ),
                ],
            )
            client.roles.assign(user="custom-user", roles=role_name)
            assert (
                len(source) == 1
            )  # uses aggregate in background, cannot do that with restricted user

            source_no_rights = client_no_rights.collections.get(
                source.name
            )  # no network call => no RBAC check

            ret = source_no_rights.data.delete_many(
                where=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
            )
            assert ret.successful == 1
            assert (
                len(source) == 0
            )  # uses aggregate in background, cannot do that with restricted user
            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        # read+delete for just one
        for col in [source.name, target.name]:
            with weaviate.connect_to_local(
                port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
            ) as client_no_rights:
                uuid_source = source.data.insert(properties={}, references={"ref": uuid_target1})

                source.data.reference_add(
                    from_uuid=uuid_source,
                    from_property="ref",
                    to=uuid_target1,
                )
                client.roles.create(
                    name=role_name,
                    permissions=[
                        RBAC.permissions.collections.objects.delete(
                            collection=col,
                        ),
                        RBAC.permissions.collections.read(
                            collection=col,
                        ),
                    ],
                )
                client.roles.assign(user="custom-user", roles=role_name)
                assert (
                    len(source) == 1
                )  # uses aggregate in background, cannot do that with restricted user

                source_no_rights = client_no_rights.collections.get(
                    source.name
                )  # no network call => no RBAC check

                # deletion does not work
                with pytest.raises(weaviate.exceptions.WeaviateQueryException) as e:
                    source_no_rights.data.delete_many(
                        where=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
                    )
                assert "forbidden" in e.value.args[0]
                assert (
                    len(source) == 1
                )  # uses aggregate in background, cannot do that with restricted user

                # delete from admin-collection so next iteration is "clean" again
                source.data.delete_many(
                    where=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
                )

                client.roles.revoke(user="custom-user", roles=role_name)
                client.roles.delete(role_name)

        client.collections.delete([target.name, source.name])


def test_search_with_filter_and_return(request: SubRequest) -> None:
    col_name = _sanitize_role_name(request.node.name)

    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        client.collections.delete([col_name + "target", col_name + "source"])
        # create two collections with some objects to test refs
        target = client.collections.create(
            name=col_name + "target",
            properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
        )
        source = client.collections.create(
            name=col_name + "source",
            references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        )
        uuid_target1 = target.data.insert({"prop": "word"})
        source.data.insert(properties={}, references={"ref": uuid_target1})

        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)

        # read for both
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(
                name=role_name,
                permissions=[
                    RBAC.permissions.collections.read(
                        collection=target.name,
                    ),
                    RBAC.permissions.collections.read(
                        collection=source.name,
                    ),
                    RBAC.permissions.collections.objects.read(
                        collection=source.name,
                    ),
                    RBAC.permissions.collections.objects.read(
                        collection=target.name,
                    ),
                ],
            )
            client.roles.assign(user="custom-user", roles=role_name)

            source_no_rights = client_no_rights.collections.get(
                source.name
            )  # no network call => no RBAC check

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

            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        # read for just one
        for col in [source.name, target.name]:
            with weaviate.connect_to_local(
                port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
            ) as client_no_rights:
                client.roles.create(
                    name=role_name,
                    permissions=RBAC.permissions.collections.read(
                        collection=col,
                    ),
                )
                client.roles.assign(user="custom-user", roles=role_name)

                source_no_rights = client_no_rights.collections.get(
                    source.name
                )  # no network call => no RBAC check

                with pytest.raises(weaviate.exceptions.WeaviateQueryException) as e:
                    source_no_rights.query.fetch_objects(
                        filters=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
                    )
                assert "forbidden" in e.value.args[0]

                with pytest.raises(weaviate.exceptions.WeaviateQueryException) as e:
                    source_no_rights.query.fetch_objects(
                        return_references=wvc.query.QueryReference(
                            link_on="ref",
                            return_properties=["prop"],
                        ),
                    )
                assert "forbidden" in e.value.args[0]

                client.roles.revoke(user="custom-user", roles=role_name)
                client.roles.delete(role_name)

        client.collections.delete([target.name, source.name])


def test_batch_ref(request: SubRequest):
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        col_name = _sanitize_role_name(request.node.name)
        client.collections.delete([col_name + "target1", col_name + "target2", col_name + "source"])
        # create two collections with some objects to test refs
        target1 = client.collections.create(name=col_name + "target1")
        target2 = client.collections.create(name=col_name + "target2")
        source = client.collections.create(
            name=col_name + "source",
            references=[
                wvc.config.ReferenceProperty(name="ref1", target_collection=target1.name),
                wvc.config.ReferenceProperty(name="ref2", target_collection=target2.name),
            ],
        )
        source.config.add_reference(
            wvc.config.ReferenceProperty(name="self", target_collection=source.name)
        )

        uuid_target1 = target1.data.insert({})
        uuid_target2 = target2.data.insert({})
        uuid_source = source.data.insert(properties={})
        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)

        # self reference
        self_permissions = [
            RBAC.permissions.collections.read(collection=source.name),
            RBAC.permissions.collections.objects.update(collection=source.name),
        ]
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(name=role_name, permissions=self_permissions)
            client.roles.assign(user="custom-user", roles=role_name)

            source_no_rights = client_no_rights.collections.get(
                source.name
            )  # no network call => no RBAC check
            ret = source_no_rights.data.reference_add_many(
                [DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source)]
            )
            assert len(ret.errors) == 0

            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            for permissions in generate_missing_lists(self_permissions):
                role = client.roles.create(name=role_name, permissions=permissions)
                client.roles.assign(user="custom-user", roles=role.name)

                source_no_rights = client_no_rights.collections.get(
                    source.name
                )  # no network call => no RBAC check
                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    source_no_rights.data.reference_add_many(
                        [
                            DataReference(
                                from_property="self", from_uuid=uuid_source, to_uuid=uuid_source
                            )
                        ]
                    )
                assert e.value.status_code == 403

                client.roles.revoke(user="custom-user", roles=role_name)
                client.roles.delete(role_name)

        # ref to one target
        ref1_permissions = [
            RBAC.permissions.collections.read(collection=source.name),
            RBAC.permissions.collections.objects.update(collection=source.name),
            RBAC.permissions.collections.read(collection=target1.name),
        ]
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(name=role_name, permissions=ref1_permissions)
            client.roles.assign(user="custom-user", roles=role_name)

            source_no_rights = client_no_rights.collections.get(source.name)
            ret = source_no_rights.data.reference_add_many(
                [
                    DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                    DataReference(
                        from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1
                    ),
                ]
            )
            assert len(ret.errors) == 0

            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        # without read permission for target that one reference fails
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            # no rights to read target class
            role = client.roles.create(name=role_name, permissions=ref1_permissions[:-1])
            client.roles.assign(user="custom-user", roles=role.name)

            source_no_rights = client_no_rights.collections.get(source.name)
            ret = source_no_rights.data.reference_add_many(
                [
                    DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                    DataReference(
                        from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1
                    ),
                ]
            )

            assert len(ret.errors) == 1
            assert "forbidden" in ret.errors[1].message

            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        # ref to two targets
        ref2_permissions = [
            RBAC.permissions.collections.read(collection=source.name),
            RBAC.permissions.collections.objects.update(collection=source.name),
            RBAC.permissions.collections.read(collection=target1.name),
            RBAC.permissions.collections.read(collection=target2.name),
        ]
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(name=role_name, permissions=ref2_permissions)
            client.roles.assign(user="custom-user", roles=role_name)

            source_no_rights = client_no_rights.collections.get(source.name)
            ret = source_no_rights.data.reference_add_many(
                [
                    DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                    DataReference(
                        from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1
                    ),
                    DataReference(
                        from_property="ref2", from_uuid=uuid_source, to_uuid=uuid_target2
                    ),
                ]
            )
            assert len(ret.errors) == 0

            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        # without read permission for target that one reference fails
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            # no rights to read target class
            role = client.roles.create(name=role_name, permissions=ref2_permissions[:-1])
            client.roles.assign(user="custom-user", roles=role.name)

            source_no_rights = client_no_rights.collections.get(source.name)
            ret = source_no_rights.data.reference_add_many(
                [
                    DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                    DataReference(
                        from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1
                    ),
                    DataReference(
                        from_property="ref2", from_uuid=uuid_source, to_uuid=uuid_target2
                    ),
                ]
            )

            # only target where we miss permissions is failing
            assert len(ret.errors) == 1
            assert "forbidden" in ret.errors[2].message

            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        # data permission is not needed as the target uuid is not checked for existence
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(name=role_name, permissions=ref2_permissions)
            client.roles.assign(user="custom-user", roles=role_name)

            source_no_rights = client_no_rights.collections.get(source.name)
            ret = source_no_rights.data.reference_add_many(
                [
                    DataReference(from_property="self", from_uuid=uuid_source, to_uuid=uuid_source),
                    DataReference(
                        from_property="ref1", from_uuid=uuid_source, to_uuid=uuid_target1
                    ),
                    DataReference(  # silently fails => target collection is not accessed to check
                        from_property="ref2", from_uuid=uuid_source, to_uuid=uuid.uuid4()
                    ),
                ]
            )
            assert len(ret.errors) == 0

            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)
