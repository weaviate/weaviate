import datetime
from typing import Optional

import httpx
import pytest
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
                "name": "Object " + str(i),
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


@pytest.mark.parametrize(
    "ttl,expected_count",
    [
        (
            datetime.timedelta(hours=2),
            11,
        ),  # use 2 hours AFTER the date property, so none are expired
        (datetime.timedelta(seconds=0), 6),
        (
            datetime.timedelta(hours=-2),
            0,
        ),  # use 2 hours BEFORE the date property, so all are expired
    ],
)
def test_post_search_filter(
    collection_factory: CollectionFactory, ttl: datetime.timedelta, expected_count: int
) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_date_property(
            date_property="custom_date",
            post_search_filter=True,
            time_to_live_after_date=ttl,
        ),
    )

    # add a bunch of expired objects, but don't delete them yet
    num_expired_objects = 5
    for i in range(num_expired_objects):
        collection.data.insert(
            properties={
                "custom_date": datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(hours=1),
            }
        )

    # add a bunch of NOT expired objects
    num_not_expired_objects = 6
    for i in range(num_not_expired_objects):
        collection.data.insert(
            properties={
                "custom_date": datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(hours=1),
            }
        )

    results = collection.query.fetch_objects()
    assert len(results.objects) == expected_count
