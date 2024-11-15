import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name


def test_rbac_users(request: SubRequest):
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
        uuid_source = source.data.insert(properties={}, references={"ref": uuid_target1})
        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)

        # read+update for both
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            both_write = client.roles.create(
                name=role_name,
                permissions=RBAC.permissions.collections(
                    collection=target.name, actions=RBAC.actions.collection.UPDATE
                )
                + RBAC.permissions.collections(collection=target.name, actions=RBAC.actions.collection.READ)
                + RBAC.permissions.collections(collection=source.name, actions=RBAC.actions.collection.UPDATE)
                + RBAC.permissions.collections(collection=source.name, actions=RBAC.actions.collection.READ),
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

        # only read+update for one of them
        for col in [source.name, target.name]:
            with weaviate.connect_to_local(
                port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
            ) as client_no_rights:
                role = client.roles.create(
                    name=role_name,
                    permissions=RBAC.permissions.collections(
                        collection=col, actions=RBAC.actions.collection.UPDATE
                    )
                    + RBAC.permissions.collections(collection=col, actions=RBAC.actions.collection.READ),
                )
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
                permissions=RBAC.permissions.collections(
                    collection=target.name, actions=RBAC.actions.collection.DELETE
                )
                + RBAC.permissions.collections(collection=target.name, actions=RBAC.actions.collection.READ)
                + RBAC.permissions.collections(collection=source.name, actions=RBAC.actions.collection.DELETE)
                + RBAC.permissions.collections(collection=source.name, actions=RBAC.actions.collection.READ),
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
                    permissions=RBAC.permissions.collections(
                        collection=col, actions=RBAC.actions.collection.DELETE
                    )
                    + RBAC.permissions.collections(collection=col, actions=RBAC.actions.collection.READ),
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


def test_search_with_filter(request: SubRequest) -> None:
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
        uuid_source = source.data.insert(properties={}, references={"ref": uuid_target1})
        source.data.reference_add(
            from_uuid=uuid_source,
            from_property="ref",
            to=uuid_target1,
        )

        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)

        # read for both
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(
                name=role_name,
                permissions=RBAC.permissions.collections(
                    collection=target.name, actions=RBAC.actions.collection.READ
                )
                + RBAC.permissions.collections(collection=source.name, actions=RBAC.actions.collection.READ),
            )
            client.roles.assign(user="custom-user", roles=role_name)

            source_no_rights = client_no_rights.collections.get(
                source.name
            )  # no network call => no RBAC check

            ret = source_no_rights.query.fetch_objects(
                filters=wvc.query.Filter.by_ref("ref").by_id().equal(uuid_target1)
            )
            assert len(ret.objects) == 1
            client.roles.revoke(user="custom-user", roles=role_name)
            client.roles.delete(role_name)

        # read for just one
        for col in [source.name, target.name]:
            with weaviate.connect_to_local(
                port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
            ) as client_no_rights:
                client.roles.create(
                    name=role_name,
                    permissions=RBAC.permissions.collections(
                        collection=col, actions=RBAC.actions.collection.READ
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

                client.roles.revoke(user="custom-user", roles=role_name)
                client.roles.delete(role_name)

        client.collections.delete([target.name, source.name])
