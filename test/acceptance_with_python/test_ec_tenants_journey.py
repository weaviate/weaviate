import tqdm
import weaviate.classes as wvc
from conftest import CollectionFactory

def test_eventual_consistency_journey(collection_factory: CollectionFactory) -> None:
    '''Test the eventual consistency functionality of a multi-node RAFT cluster.

    This test should only be run if the cluster is configured with at least 3 nodes.
    '''
    collection = collection_factory(
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(),
        properties=[
            wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="content", data_type=wvc.config.DataType.TEXT),
        ],
        replication_config=wvc.config.Configure.replication(factor=3),
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none(name="bringYourOwn"),
        ],
    )

    for i in tqdm.trange(0, 100):
        tenant_name = f"tenant_{i}"
        collection.tenants.create([wvc.tenants.Tenant(name=tenant_name)])
        tenant = collection.with_tenant(tenant_name)
        ret = tenant.data.insert_many([wvc.data.DataObject(
            properties={"title": "Hello", "content": "World"},
            vector={"bringYourOwn": [0.5, 0.25, 0.75]},
        ) for _ in range(0, 1000)])
        objs = tenant.query.fetch_objects(filters=wvc.query.Filter.by_id().contains_any(list(ret.uuids.values())), limit=2000).objects
        assert len(objs) == 1000