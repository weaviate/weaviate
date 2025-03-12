import random
import math
from loguru import logger
import weaviate
import weaviate.classes as wvc
import pytest


# The idea is that we append this number to collection or prop names. It is
# meant to return the same number a lot of tiems, but not always. This way,
# when it returns a number it has returned before, we're likely to hit an
# existing col or prop, but occasionally we'll get something new.
def random_number_with_frequent_collisions():
    return math.floor(math.log(random.randint(1, 1000000), 2))


@pytest.mark.skip(
    reason="currently python integration test run on a single node cluster, this test needs a dedicated 3 node cluster"
)
def test_auto_schema_explicit_property_update_ec(weaviate_client) -> None:
    clients: [weaviate.WeaviateClient] = [weaviate_client(8080 + 1, 50051 + 1) for i in range(3)]
    client = random.choice(clients)
    client.collections.delete_all()
    logger.info("cleanup completed")

    for col_iter in range(25):
        client = random.choice(clients)
        col_name = f"ExplicitCollection{col_iter}"
        logger.info(f"start collection {col_name}")
        client.collections.create(
            col_name,
            vectorizer_config=wvc.config.Configure.Vectorizer.none(),
            sharding_config=wvc.config.Configure.sharding(desired_count=random.randint(1, 3)),
            replication_config=wvc.config.Configure.replication(factor=random.randint(1, 3)),
        )
        for i in range(50):
            if i % 10 == 0:
                logger.info(f"start iteration {i}")

            client: weaviate.WeaviateClient = random.choice(clients)
            with client.batch.fixed_size(10) as b:
                for j in range(10):
                    text_prop = f"text_{random_number_with_frequent_collisions()}"
                    number_prop = f"number_{random_number_with_frequent_collisions()}"
                    obj = {
                        text_prop: text_prop,
                        number_prop: random.randint(0, 10000),
                    }
                    b.add_object(col_name, obj)
            assert len(client.batch.failed_objects) == 0
