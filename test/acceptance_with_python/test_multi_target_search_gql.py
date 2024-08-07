import weaviate
import weaviate.classes as wvc
import math


from .conftest import CollectionFactory, NamedCollection

GQL_RETURNS = "{_additional {distance id score}"
GQL_TARGETS = 'targets: {targetVectors: ["title1", "title2", "title3"], combinationMethod: sum}'
CAR_DISTANCE = 0.7892138957977295
APPLE_DISTANCE = 0.5168729424476624
KALE_DISTANCE = 0.5732871294021606


def test_gql_near_text(named_collection: NamedCollection):
    collection = named_collection()
    collection.data.insert(properties={"title1": "apple", "title2": "car", "title3": "kale"})

    # use collection for auto cleanup etc, but we need the client to use gql directly
    client = weaviate.connect_to_local()
    gql = client.graphql_raw_query(
        """{
      Get {
        """
        + collection.name
        + """(
          nearText: {
            concepts: ["fruit"]
            """
        + GQL_TARGETS
        + """
          }
        ) """
        + GQL_RETURNS
        + """
        }
      }
    }"""
    )

    assert math.isclose(
        gql.get[collection.name][0]["_additional"]["distance"],
        CAR_DISTANCE + APPLE_DISTANCE + KALE_DISTANCE,
        rel_tol=1e-5,
    )


def test_gql_near_vector(named_collection: NamedCollection):
    collection = named_collection()
    collection.data.insert(
        properties={"title1": "first"},
        vector={
            "title1": [1, 0, 0],
            "title2": [0, 0, 1],
            "title3": [1, 0, 0],
        },
    )

    # use collection for auto cleanup etc, but we need the client to use gql directly
    client = weaviate.connect_to_local()
    gql = client.graphql_raw_query(
        """{
      Get {
        """
        + collection.name
        + """(
          nearVector: {
            vector: [0, 0, 1]
            """
        + GQL_TARGETS
        + """
          }
        ) """
        + GQL_RETURNS
        + """
        }
      }
    }"""
    )

    assert gql.get[collection.name][0]["_additional"]["distance"] == 2


def test_gql_near_object(named_collection: NamedCollection):
    collection = named_collection()
    uuid1 = collection.data.insert(
        properties={"title1": "first"},
        vector={
            "title1": [1, 0, 0],
            "title2": [0, 0, 1],
            "title3": [1, 0, 0],
        },
    )
    uuid2 = collection.data.insert(
        properties={"title1": "second"},
        vector={
            "title1": [1, 0, 0],
            "title2": [0, 0, 1],
            "title3": [0, 1, 0],
        },
    )

    uuid_str = '"' + str(uuid1) + '"'
    # use collection for auto cleanup etc, but we need the client to use gql directly
    with weaviate.connect_to_local() as client:
        gql = client.graphql_raw_query(
            """{
            Get {
                """
            + collection.name
            + """(
            nearObject: {
                id: """
            + uuid_str
            + """
                """
            + GQL_TARGETS
            + """
            }
            ) """
            + GQL_RETURNS
            + """
            }
        }
        }"""
        )

    assert gql.get[collection.name][0]["_additional"]["distance"] == 0
    assert gql.get[collection.name][1]["_additional"]["distance"] == 1


def test_test_multi_target_near_vector_gql(collection_factory: CollectionFactory):
    collection = collection_factory(
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none(
                name=entry,
            )
            for entry in ["title1", "title2", "title3"]
        ]
    )

    collection.data.insert(
        properties={}, vector={"title1": [1, 0], "title2": [0, 0, 1], "title3": [0, 0, 0, 1]}
    )
    uuid2 = collection.data.insert(
        properties={}, vector={"title1": [0, 1], "title2": [0, 1, 0], "title3": [0, 0, 1, 0]}
    )

    client = weaviate.connect_to_local()
    gql = client.graphql_raw_query(
        """{
      Get {
        """
        + collection.name
        + """(
          nearVector: {
            vectorPerTarget: {title1: [0, 1], title2: [0, 1, 0], title3: [0, 0, 1, 0]}
            distance: 0.1
            """
        + GQL_TARGETS
        + """
          }
        ) """
        + GQL_RETURNS
        + """
        }
      }
    }"""
    )
    assert gql.get[collection.name][0]["_additional"]["distance"] == 0
    assert gql.get[collection.name][0]["_additional"]["id"] == str(uuid2)


def test_test_multi_target_hybrid_gql(collection_factory: CollectionFactory):
    collection = collection_factory(
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none(
                name=entry,
            )
            for entry in ["title1", "title2", "title3"]
        ]
    )

    uuid0 = collection.data.insert(
        properties={}, vector={"title1": [1, 0], "title2": [0, 0, 1], "title3": [0, 0, 0, 1]}
    )
    uuid1 = collection.data.insert(
        properties={}, vector={"title1": [0, 1], "title2": [0, 1, 0], "title3": [0, 0, 1, 0]}
    )
    uuid2 = collection.data.insert(
        properties={}, vector={"title1": [1, 0], "title2": [1, 0, 0], "title3": [0, 1, 0, 0]}
    )

    client = weaviate.connect_to_local()

    gql_query = (
        """{
          Get {
            """
        + collection.name
        + """(
              hybrid: {
                alpha:1
                searches: { nearVector:{
                    vectorPerTarget: {title1: [1, 0], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
                    distance: 3.1
                }}
                """
        + GQL_TARGETS
        + """
              }
            ) """
        + GQL_RETURNS
        + """
            }
          }
    }"""
    )

    gql = client.graphql_raw_query(gql_query)
    assert gql.get[collection.name][0]["_additional"]["score"] == "1"
    assert gql.get[collection.name][0]["_additional"]["id"] == str(uuid0)
    assert gql.get[collection.name][1]["_additional"]["score"] == "0.33333334"
    assert gql.get[collection.name][1]["_additional"]["id"] == str(uuid2)
    assert gql.get[collection.name][2]["_additional"]["score"] == "0"
    assert gql.get[collection.name][2]["_additional"]["id"] == str(uuid1)
