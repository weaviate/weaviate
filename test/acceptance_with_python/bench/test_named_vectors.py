import random

import pytest
from typing_extensions import Any, List
from weaviate.collections.classes.data import DataObject

from ..conftest import NamedCollection

# run with
# pytest test/acceptance_with_python/bench/test_named_vectors.py --benchmark-only --benchmark-disable-gc


def compute_vectors():
    # seed with same number to keep reproducible - this is only used to shuffle vectors
    random.seed(4)
    base_vector = [0.01 * i for i in range(100)]

    def shuffle(input: List[float]) -> List[float]:
        random.shuffle(input)
        return input

    return [shuffle(base_vector) for _ in range(1500)]


# precompute outside of benchmark so that this does not change the measured times

INPUT_VECTORS = compute_vectors()


def named_vectors(named_collection: NamedCollection, num_targets: int) -> None:
    collection = named_collection()
    target_vectors = ["All", "title1", "title2"]

    num_objects = 100
    ret = collection.data.insert_many(
        [
            DataObject(
                vector={
                    "All": INPUT_VECTORS[i * 3],
                    "title1": INPUT_VECTORS[i * 3 + 1],
                    "title2": INPUT_VECTORS[i * 3 + 2],
                }
            )
            for i in range(num_objects)
        ]
    )
    assert not ret.has_errors

    query_vecs = INPUT_VECTORS[num_objects * 3 :]
    for j in range(len(query_vecs)):
        collection.query.near_vector(query_vecs[j], target_vector=target_vectors[:num_targets])


@pytest.mark.parametrize("num_targets", [1, 2, 3])
def test_benchmark_named_vectors(
    benchmark: Any, named_collection: NamedCollection, num_targets: int
) -> None:
    benchmark(named_vectors, named_collection, num_targets)
