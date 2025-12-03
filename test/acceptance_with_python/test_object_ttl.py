import datetime
from typing import Optional

import httpx
import weaviate.classes as wvc
from weaviate.classes.config import Configure

from .conftest import CollectionFactory


def delete(expiration_time: Optional[datetime.datetime] = None):
    with httpx.Client() as client:
        params = {}
        if expiration_time is not None:
            if expiration_time.tzinfo is None:
                expiration_time = expiration_time.replace(tzinfo=datetime.timezone.utc)

            params["expiration"] = expiration_time.isoformat(sep="T", timespec="microseconds")

        response = client.get(
            "http://localhost:6060/debug/ttl/deleteall", params=params, timeout=30
        )
        response.raise_for_status()


def test_custom_property(
    collection_factory: CollectionFactory,
) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_date_property(
            date_property="custom_date",
        ),
    )
    num_objects = 5
    for i in range(num_objects):
        collection.data.insert(
            {
                "name": "Object 1",
                "custom_date": datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=i),
            }
        )

    for i in range(num_objects):
        delete(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=i))
        assert len(collection) == num_objects - (i + 1)


# def test_update_time(collection_factory: CollectionFactory):
#     collection = collection_factory(
#         properties=[
#             wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
#         ],
#         object_ttl=Configure.ObjectTTL.delete_by_update_time(
#             time_to_live=datetime.timedelta(minutes=1),
#         ),
#         inverted_index_config=Configure.inverted_index(index_timestamps=True),
#     )
#
#     uuids = []
#     for i in range(5):
#         uid = collection.data.insert(properties={"name": "Object" + str(i)})
#         uuids.append(uid)
#     start = datetime.datetime.now(datetime.timezone.utc)
#     time.sleep(5)
#     for i, uid in enumerate(uuids[:3]):
#         collection.data.update(properties={"name": "Object" + str(i) + "new"}, uuid=uid)
#
#     delete(start + datetime.timedelta(minutes=1))
#     assert len(collection) == 2
