import pytest
from weaviate.collections.classes.config import _ReplicationConfigCreate

from .conftest import CollectionFactory
import weaviate.classes as wvc


@pytest.mark.parametrize(
    "replication_config,expected",
    [
        (None, True),
        (wvc.config.Configure.replication(factor=1), True),
        (wvc.config.Configure.replication(async_enabled=False), False),
        (wvc.config.Configure.replication(async_enabled=True), True),
    ],
)
def test_replication(
    collection_factory: CollectionFactory,
    replication_config: _ReplicationConfigCreate,
    expected: bool,
) -> None:
    collection = collection_factory(replication_config=replication_config)
    assert collection.config.get().replication_config.async_enabled == expected
