import weaviate
import weaviate.classes as wvc

def test_collection_casing() -> None:
    with weaviate.connect_to_local() as client:
        # Goal: Collection create/get/delete should automatically transform class name to GQL (first letter caps).

        # create collection with all "lower" case -> GQL ("Testcollectioncase")
        assert client.collections.create_from_dict({
            "class": "testcollectioncase",
            "vectorizer": "none",
            }
        ) is not None

        # GET should also tranform to GQL
        assert client.collections.get("testcollectioncase") is not None

        # DELETE should also transform to GQL
        client.collections.delete("testcollectioncase")
        col = client.collections.get("testcollectioncase")
        assert col.exists() is False

        # same with mult-word with "_"
        assert client.collections.create_from_dict({
            "class": "test_collection_case",
            "vectorizer": "none",
            }
        ) is not None


        # GET should also tranform to GQL
        assert client.collections.get("test_collection_case") is not None

        # DELETE should also transform to GQL
        client.collections.delete("test_collection_case")
        col = client.collections.get("testcollectioncase")
        assert col.exists() is False



# @pytest.mark.parametrize("to_upper", [True, False])
# def test_rbac_refs(request: SubRequest, to_upper: bool):
#     with weaviate.connect_to_local(
#         port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
#     ) as client:
#         name = _sanitize_role_name(request.node.name)
#         if to_upper:
#             name = name[0].upper() + name[1:]
#         client.collections.delete(name)
#         client.roles.delete(name)
#         collection = client.collections.create(name=name)

#         with weaviate.connect_to_local(
#             port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
#         ) as client_no_rights:
#             client.roles.create(
#                 name=name,
#                 permissions=[
#                     RBAC.permissions.collections.read(collection=name),
#                     RBAC.permissions.collections.objects.read(collection=name),
#                 ],
#             )
#             client.roles.assign(user="custom-user", roles=name)
#             collection_no_rights = client_no_rights.collections.get(collection.name)
#             collection_no_rights.query.fetch_objects()

#             client.roles.revoke(user="custom-user", roles=name)
#             client.roles.delete(name)

#         client.collections.delete(name)
