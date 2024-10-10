## What is this?

tldr; use `cmd/weaviate-server` instead. This is experimental

## Architecture

![Architecture](./images/architecture.png?raw=true "Architecture")

## Setup

### Start Weaviate core.
```sh
./tools/dev/restart_dev_environment.sh --prometheus --s3 --contextionary && ./tools/dev/run_dev_server.sh local-node-with-offload
```

This also starts other dependencies like `contextionary` (for vectorizing), `minio` (used as object storage) and `Prometheus` with `Grafana` for monitoring.

### Start Weaviate Querier

``` sh
export OFFLOAD_S3_BUCKET_AUTO_CREATE=true
export OFFLOAD_S3_ENDPOINT=http://localhost:9000
export AWS_SECRET_KEY=aws_secret_key
export AWS_ACCESS_KEY=aws_access_key
go build ./cmd/weaviate && ./weaviate --target=querier
```

Weaviate `querier` has some sane default configs. To tweak the configs use `./weaviate --target=querier --help`

``` sh
-bash-5.2$ ./weaviate --target=querier --help
Usage:
  weaviate [OPTIONS]

Application Options:
      --target=               how should weaviate-server be running as e.g: querier, ingester, etc

query:
      --query.grpc.listen=    gRPC address that query node listens at (default: 0.0.0.0:9091)
      --query.schema.addr=    address to get schema information (default: http://0.0.0.0:8080)
      --query.s3.url=         s3 URL to query offloaded tenants (e.g: s3://<url>)
      --query.s3.endpoint=    s3 endpoint to if mocking s3 (e.g: via minio)
      --query.datapath=       place to look for tenant data after downloading it from object storage (default: /tmp)
      --query.vectorize-addr= vectorizer address to be used to vectorize near-text query (default: 0.0.0.0:9999)

Help Options:
  -h, --help                  Show this help message

```

## Ingestion (on Weaviate core)

``` python
import weaviate
from weaviate.classes.init import AdditionalConfig, Timeout
from weaviate.classes.config import Configure
from weaviate.classes.config import Property, DataType
from weaviate.classes.tenants import Tenant

with weaviate.connect_to_local(
    grpc_port=50051, # weaviate core
    # grpc_port=9091,
    additional_config=AdditionalConfig(
        timeout=Timeout(init=30, query=60, insert=120)  # Values in seconds
    ),
    skip_init_checks=True,
) as client:

    # Create collection
    multi_collection = client.collections.create(
        "Question",
        multi_tenancy_config=Configure.multi_tenancy(True),
        vector_index_config=Configure.VectorIndex.flat(
            quantizer=Configure.VectorIndex.Quantizer.bq(
                rescore_limit=200,
                cache=True
            ),
        ),
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(),
        properties=[
            Property(name="answer", data_type=DataType.TEXT),
            Property(name="question", data_type=DataType.TEXT),
            Property(name="category", data_type=DataType.TEXT),
        ]
    )

    print("created class 'Question`")

    # Create tenant
    multi_collection.tenants.create(
        tenants=[
            Tenant(name="weaviate-tenant"),
        ]
    )

    print("created tenant 'weaviate-tenant`")

    # Ingest objects

    multi_collection = client.collections.get("Question").with_tenant("weaviate-tenant")

    objects = [
    {
        "answer": "Liver",
        "question": "This organ removes excess glucose from the blood & stores it as glycogen",
        "category": "SCIENCE"
    },
    {
        "answer": "Elephant",
        "question": "It's the only living mammal in the order Proboseidea",
        "category": "ANIMALS"
    },
    {
        "answer": "the nose or snout",
        "question": "The gavial looks very much like a crocodile except for this bodily feature",
        "category": "ANIMALS"
    },
    {
        "answer": "Antelope",
        "question": "Weighing around a ton, the eland is the largest species of this animal in Africa",
        "category": "ANIMALS"
    },
    {
        "answer": "the diamondback rattler",
        "question": "Heaviest of all poisonous snakes is this North American rattlesnake",
        "category": "ANIMALS"
    },
    {
        "answer": "species",
        "question": "2000 news: the Gunnison sage grouse isn't just another northern sage grouse, but a new one of this classification",
        "category": "SCIENCE"
    },
    {
        "answer": "wire",
        "question": "A metal that is ductile can be pulled into this while cold & under pressure",
        "category": "SCIENCE"
    },
    {
        "answer": "DNA",
        "question": "In 1953 Watson & Crick built a model of the molecular structure of this, the gene-carrying substance",
        "category": "SCIENCE"
    },
    {
        "answer": "the atmosphere",
        "question": "Changes in the tropospheric layer of this are what gives us weather",
        "category": "SCIENCE"
    },
    {
        "answer": "Sound barrier",
        "question": "In 70-degree air, a plane traveling at about 1,130 feet per second breaks it",
        "category": "SCIENCE"
    }
  ]

    with multi_collection.batch.dynamic() as batch:
        for object in objects:
            batch.add_object(
                properties=object,
            )

    print("ingested objects")

```

## Offload tenants (on Weaviate core)

``` python
import weaviate
from weaviate.classes.init import AdditionalConfig, Timeout
from weaviate.classes.tenants import Tenant, TenantActivityStatus

with weaviate.connect_to_local(
    grpc_port=50051, # weaviate core
    # grpc_port=9091,
    additional_config=AdditionalConfig(
        timeout=Timeout(init=30, query=60, insert=120)  # Values in seconds
    ),
    skip_init_checks=True,
) as client:

    # Offload the tenants
    multi_collection = client.collections.get("Question")
    multi_collection.tenants.update(tenants=[
        Tenant(
            name="weaviate-tenant",
            activity_status=TenantActivityStatus.OFFLOADED
        )
    ])

    print("offloaded tenant 'weaviate-tenant`")

```

## Querying (on Weaviate querier)

### Vector search

``` python
import weaviate
from weaviate.classes.init import AdditionalConfig, Timeout

with weaviate.connect_to_local(
    grpc_port=9091,
    additional_config=AdditionalConfig(
        timeout=Timeout(init=30, query=60, insert=120)  # Values in seconds
    ),
    skip_init_checks=True,
) as client:

    questions = client.collections.get("Question")

    response = questions.with_tenant("weaviate-tenant").query.near_text(
        query="biology",
        certainty=0.6,
    )

    for obj in response.objects:
        print(obj.properties)

```

### Proper Filters

``` python
import weaviate
from weaviate.connect import ConnectionParams
from weaviate.classes.init import AdditionalConfig, Timeout, Auth
from weaviate.classes.query import Filter
from weaviate.classes.config import Configure
import os

with weaviate.connect_to_local(
    grpc_port=9091,
    additional_config=AdditionalConfig(
        timeout=Timeout(init=30, query=60, insert=120)  # Values in seconds
    ),
    skip_init_checks=True,
) as client:

    questions = client.collections.get("Question")

    response = questions.with_tenant("weaviate-tenant").query.fetch_objects(
        filters=(
            Filter.by_property("category").equal("SCIENCE") &
            Filter.by_property("answer").equal("Liver")
        ),
    )

    for obj in response.objects:
        print(obj.properties)

```

## TODO(s)

- [ ] Handle "empty write-ahead-log found" warnings on `querier`
- [ ] Support BM25 search
- [ ] Support other indexes other than flat index with Binary Quantization(BQ)
- [ ] Integrate `query.Search` with core weaviate's http, grpc and graphql endpoints for frozen tenants.
- [x] Instrument `grpc` server
- [x] Instrument `http` server
- [ ] Log query metadata (add it in `slow_queries.go`)
- [ ] Grafana dashboards
- [ ] Alerts
- [ ] Playbook for each alerts.
- [ ] Demo video of running core + querier to query offloaded tenants.
