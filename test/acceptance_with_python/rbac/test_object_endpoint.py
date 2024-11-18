import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name


def test_obj_insert(request: SubRequest):
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        name = _sanitize_role_name(request.node.name)
        client.collections.delete(name)
        client.roles.delete(name)
        col = client.collections.create(name=name)

        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(
                name=name,
                permissions=RBAC.permissions.collections_data(
                    collection=col.name, actions=RBAC.actions.collection_data.CREATE
                )
                + RBAC.permissions.collections(
                    collection=col.name, actions=RBAC.actions.collection.READ
                ),
            )
            client.roles.assign(user="custom-user", roles=name)

            source_no_rights = client_no_rights.collections.get(
                name
            )  # no network call => no RBAC check

            source_no_rights.data.insert({})

            client.roles.revoke(user="custom-user", roles=name)
            client.roles.delete(name)

        # no metadata read
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            both_write = client.roles.create(
                name=name,
                permissions=RBAC.permissions.collections_data(
                    collection=col.name, actions=RBAC.actions.collection_data.CREATE
                ),
            )
            client.roles.assign(user="custom-user", roles=both_write.name)

            source_no_rights = client_no_rights.collections.get(
                name
            )  # no network call => no RBAC check
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                source_no_rights.data.insert({})
            assert e.value.status_code == 403

            client.roles.revoke(user="custom-user", roles=both_write.name)
            client.roles.delete(both_write.name)

        # no data create
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            both_write = client.roles.create(
                name=name,
                permissions=RBAC.permissions.collections(
                    collection=col.name, actions=RBAC.actions.collection.READ
                ),
            )
            client.roles.assign(user="custom-user", roles=both_write.name)

            source_no_rights = client_no_rights.collections.get(
                name
            )  # no network call => no RBAC check
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                source_no_rights.data.insert({})
            assert e.value.status_code == 403

            client.roles.revoke(user="custom-user", roles=both_write.name)
            client.roles.delete(both_write.name)


def test_obj_replace(request: SubRequest):
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        name = _sanitize_role_name(request.node.name)
        client.collections.delete(name)
        client.roles.delete(name)
        col = client.collections.create(name=name)

        uuid_to_replace = col.data.insert({})

        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            client.roles.create(
                name=name,
                permissions=RBAC.permissions.collections_data(
                    collection=col.name, actions=RBAC.actions.collection_data.UPDATE
                )
                + RBAC.permissions.collections(
                    collection=col.name, actions=RBAC.actions.collection.READ
                ),
            )
            client.roles.assign(user="custom-user", roles=name)

            source_no_rights = client_no_rights.collections.get(
                name
            )  # no network call => no RBAC check

            source_no_rights.data.replace(uuid=uuid_to_replace, properties={})

            client.roles.revoke(user="custom-user", roles=name)
            client.roles.delete(name)

        # no metadata read
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            both_write = client.roles.create(
                name=name,
                permissions=RBAC.permissions.collections_data(
                    collection=col.name, actions=RBAC.actions.collection_data.UPDATE
                ),
            )
            client.roles.assign(user="custom-user", roles=both_write.name)

            source_no_rights = client_no_rights.collections.get(
                name
            )  # no network call => no RBAC check
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                source_no_rights.data.replace(uuid=uuid_to_replace, properties={})
            assert e.value.status_code == 403

            client.roles.revoke(user="custom-user", roles=both_write.name)
            client.roles.delete(both_write.name)

        # no data create
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            both_write = client.roles.create(
                name=name,
                permissions=RBAC.permissions.collections(
                    collection=col.name, actions=RBAC.actions.collection.READ
                ),
            )
            client.roles.assign(user="custom-user", roles=both_write.name)

            source_no_rights = client_no_rights.collections.get(
                name
            )  # no network call => no RBAC check
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                source_no_rights.data.replace(uuid=uuid_to_replace, properties={})
            assert e.value.status_code == 403

            client.roles.revoke(user="custom-user", roles=both_write.name)
            client.roles.delete(both_write.name)
