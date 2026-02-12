import datetime
import time
from typing import Optional

import httpx
import pytest
import weaviate.classes as wvc
from weaviate.classes.config import Configure
from weaviate.collections.classes.config import Reconfigure
from weaviate.collections.classes.tenants import TenantActivityStatus

from .conftest import CollectionFactory

# dont run tests in parallel to avoid interference between manual calls to object TTL delete
pytestmark = pytest.mark.xdist_group(name="object_ttl")


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
    time.sleep(0.1)  # give some time for the deletions to be processed


@pytest.mark.parametrize("ttl_minutes", [0, 10])
def test_custom_property(collection_factory: CollectionFactory, ttl_minutes: int) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_date_property(
            property_name="custom_date",
            ttl_offset=(datetime.timedelta(minutes=ttl_minutes) if ttl_minutes > 0 else None),
        ),
    )
    base_time = datetime.datetime.now(datetime.timezone.utc)
    num_objects = 50
    for i in range(num_objects):
        collection.data.insert(
            {
                "name": "Object " + str(i),
                "custom_date": base_time + datetime.timedelta(minutes=i, seconds=5),
            }
        )

    for i in range(num_objects):
        delete(base_time + datetime.timedelta(minutes=i))
        assert len(collection) == min(num_objects - i + ttl_minutes, num_objects)


def test_obj_without_date_property(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_date_property("custom_date"),
    )

    # insert objects, some with and some without the date property
    base_time = datetime.datetime.now(datetime.timezone.utc)
    num_objects = 20
    for i in range(num_objects):
        if i % 2 == 0:
            collection.data.insert(
                {
                    "name": "Object " + str(i),
                    "custom_date": base_time - datetime.timedelta(minutes=1),
                }
            )
        else:
            collection.data.insert({"name": "Object " + str(i)})

    # all objects with the date property should be deleted and only objects without should remain
    delete(base_time)

    assert len(collection) == num_objects // 2


def test_update_time(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_update_time(
            time_to_live=datetime.timedelta(minutes=1),
        ),
        # index timestamps are not enabled, will be updated automatically due to object TTL
    )

    uuids = []
    for i in range(5):
        uid = collection.data.insert(properties={"name": "Object" + str(i)})
        uuids.append(uid)
    start = datetime.datetime.now(datetime.timezone.utc)
    time.sleep(2)
    for i, uid in enumerate(uuids[:3]):
        collection.data.update(properties={"name": "Object" + str(i) + "new"}, uuid=uid)

    delete(start + datetime.timedelta(minutes=1))
    assert len(collection) == 3  # 2 old objects that have not been updated should be deleted


def test_creation_time(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_creation_time(
            time_to_live=datetime.timedelta(minutes=1),
        ),
        inverted_index_config=Configure.inverted_index(index_timestamps=True),
    )

    for i in range(5):
        collection.data.insert(properties={"name": "Object" + str(i)})
    start = datetime.datetime.now(datetime.timezone.utc)
    time.sleep(2)
    for i in range(6):
        collection.data.insert(properties={"name": "Second batch object" + str(i)})

    delete(start + datetime.timedelta(minutes=1))
    assert len(collection) == 6


@pytest.mark.parametrize("auto_activation", [False, True])
def test_mt(collection_factory: CollectionFactory, auto_activation: bool):
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_date_property("custom_date"),
        multi_tenancy_config=Configure.multi_tenancy(enabled=True, auto_tenant_activation=auto_activation),
    )

    base_time = datetime.datetime.now(datetime.timezone.utc)

    num_tenants = 10
    num_objects = 5
    for i in range(num_tenants):
        collection.tenants.create(tenants="Tenant" + str(i))
        for j in range(num_objects):
            collection.with_tenant("Tenant" + str(i)).data.insert(
                properties={
                    "name": "Tenant " + str(i) + " Object " + str(j),
                    "custom_date": base_time + datetime.timedelta(minutes=j),
                },
            )

    # deactivate tenants
    for i in range(num_tenants):
        if i % 2 == 0:
            collection.tenants.deactivate("Tenant" + str(i))

    delete(base_time + datetime.timedelta(minutes=2))

    # deactivated tenants remain inactive
    tenants = collection.tenants.get()
    for i in range(num_tenants):
         status = tenants["Tenant" + str(i)].activity_status
         if i % 2 == 0:
            assert status == TenantActivityStatus.INACTIVE
         else:
            assert status == TenantActivityStatus.ACTIVE

    # activate tenants again
    for i in range(num_tenants):
        if i % 2 == 0:
            collection.tenants.activate("Tenant" + str(i))

    # now check the number of remaining objects per tenant
    expected_num_objects_active = num_objects - 3
    expected_num_objects_inactive = num_objects
    if auto_activation:
        expected_num_objects_inactive = num_objects - 3
    for i in range(num_tenants):
        tenant_collection = collection.with_tenant("Tenant" + str(i))
        if i % 2 == 0:
            # inactive tenants have no objects (auto=False) or expired (auto=True) objects deleted
            assert len(tenant_collection) == expected_num_objects_inactive
        else:
            # activate tenants should have expired objects deleted
            assert len(tenant_collection) == expected_num_objects_active


@pytest.mark.parametrize(
    "filter_expired_objects,ttl,expected_count",
    [
        (
            True,
            datetime.timedelta(hours=2),
            11,
        ),  # use 2 hours AFTER the date property, so none are expired
        (True, datetime.timedelta(seconds=0), 6),
        (
            True,
            datetime.timedelta(hours=-2),
            0,
        ),  # use 2 hours BEFORE the date property, so all are expired
        (False, datetime.timedelta(hours=-2), 11),
    ],
)
def test_filter_expired_objects(
    collection_factory: CollectionFactory,
    filter_expired_objects: bool,
    ttl: datetime.timedelta,
    expected_count: int,
) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
        object_ttl=Configure.ObjectTTL.delete_by_date_property(
            property_name="custom_date",
            filter_expired_objects=filter_expired_objects,
            ttl_offset=ttl,
        ),
    )

    # add a bunch of expired objects, but don't delete them yet
    num_expired_objects = 5
    for _ in range(num_expired_objects):
        collection.data.insert(
            properties={
                "custom_date": datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(hours=1),
            }
        )

    # add a bunch of NOT expired objects
    num_not_expired_objects = 6
    for _ in range(num_not_expired_objects):
        collection.data.insert(
            properties={
                "custom_date": datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(hours=1),
            }
        )

    # add one object without the date property set
    collection.data.insert(properties={"name": "no date"})

    results = collection.query.fetch_objects()
    assert len(results.objects) == expected_count + 1  # +1 for the object without date property


def test_activate_deactivate_ttl(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="custom_date", data_type=wvc.config.DataType.DATE),
        ],
    )

    # add objects to be deleted
    base_time = datetime.datetime.now(datetime.timezone.utc)
    num_objects_delete = 4
    for i in range(num_objects_delete):
        collection.data.insert(
            {
                "name": "Object " + str(i),
                "custom_date": base_time - datetime.timedelta(hours=1),
            }
        )

    num_objects_not_delete = 5
    for i in range(num_objects_not_delete):
        collection.data.insert(
            {
                "name": "Object " + str(i),
                "custom_date": base_time + datetime.timedelta(hours=1),
            }
        )

    delete(base_time)  # nothing gets deleted
    assert len(collection) == num_objects_delete + num_objects_not_delete

    collection.config.update(
        object_ttl_config=Reconfigure.ObjectTTL.delete_by_date_property(
            property_name="custom_date",
            ttl_offset=datetime.timedelta(minutes=1),
        )
    )

    delete(base_time)
    assert len(collection) == num_objects_not_delete

    # add more objects to be deleted
    for i in range(num_objects_delete):
        collection.data.insert(
            {
                "name": "Object new " + str(i),
                "custom_date": base_time - datetime.timedelta(hours=1),
            }
        )

    collection.config.update(object_ttl_config=Reconfigure.ObjectTTL.disable())
    delete(base_time)
    assert len(collection) == num_objects_not_delete + num_objects_delete
