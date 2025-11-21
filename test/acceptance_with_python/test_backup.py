import time

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.backup.backup import BackupStatus

from .conftest import _sanitize_collection_name
from _pytest.fixtures import SubRequest

pytestmark = pytest.mark.xdist_group(name="backup")


@pytest.mark.parametrize(
    "compression",
    [
        wvc.backup.BackupCompressionLevel.BEST_SPEED,
        wvc.backup.BackupCompressionLevel.ZSTD_BEST_SPEED,
    ],
)
def test_backup_and_restore(request: SubRequest, compression: wvc.backup.BackupCompressionLevel):
    name = _sanitize_collection_name(request.node.name)

    with weaviate.connect_to_local() as client:
        client.collections.delete(name)
        collection = client.collections.create(
            name=name,
            properties=[
                wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
                wvc.config.Property(name="age", data_type=wvc.config.DataType.INT),
            ],
        )

        ids = []
        for i in range(50):
            obj_id = collection.data.insert({"name": "hello" + str(i), "age": i})
            ids.append(obj_id)

        backup_id = "backup_test_" + str(int(time.time()))
        ret = client.backup.create(
            backup_id=backup_id,
            include_collections=[name],
            backend=wvc.backup.BackupStorage.FILESYSTEM,
            config=wvc.backup.BackupConfigCreate(
                compression_level=compression,
            ),
            wait_for_completion=True,
        )
        assert ret.status == BackupStatus.SUCCESS

        client.collections.delete(name)
        restore_ret = client.backup.restore(
            backup_id=backup_id,
            backend=wvc.backup.BackupStorage.FILESYSTEM,
            wait_for_completion=True,
        )
        assert restore_ret.status == BackupStatus.SUCCESS
        restored_collection = client.collections.get(name)
        assert len(restored_collection) == 50
        for i, uid in enumerate(ids):
            obj = restored_collection.query.fetch_object_by_id(uid)
            assert obj.properties["name"] == "hello" + str(i)
            assert obj.properties["age"] == i

        client.collections.delete(name)
