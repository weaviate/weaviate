
## What is this?

tldr; use `cmd/weaviate-server` instead. This is experimental

## Architecture

![Architecture](./images/architecture.png?raw=true "Architecture")

## Setup

### Start Weaviate core.
```sh
rm -rf /tmp/question
./tools/dev/restart_dev_environment.sh --prometheus --s3 --contextionary && ./tools/dev/run_dev_server.sh local-node-with-offload
```

This also starts other dependencies like `contextionary` (for vectorizing), `minio` (used as object storage) and `Prometheus` with `Grafana` for monitoring.

### Start Weaviate Querier

``` sh
export OFFLOAD_S3_BUCKET_AUTO_CREATE=true
export OFFLOAD_S3_ENDPOINT=http://localhost:9000
export AWS_SECRET_KEY=aws_secret_key
export AWS_ACCESS_KEY=aws_access_key
go build ./cmd/weaviate && ./weaviate --target=querier --query.grpc.listen=':9091'
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

### BM25 search (TODO)



## What is this?

tldr; use `cmd/weaviate-server` instead. This is experimental

## Run Locally

```sh
# reset env
./tools/dev/restart_dev_environment.sh && rm -rf minio backups /tmp/question

#shell1 (minio)
docker run --rm \
-p 9000:9000 \
-p 9001:9001 \
--user $(id -u):$(id -g) \
--name minio1 \
-e "MINIO_ROOT_USER=aws_access_key" \
-e "MINIO_ROOT_PASSWORD=aws_secret_key" \
-v ./minio/data:/data \
quay.io/minio/minio server /data --console-address ":9001"

#shell2 (weaviate0)
OFFLOAD_S3_ENDPOINT="http://localhost:9000" OFFLOAD_S3_BUCKET_AUTO_CREATE="true" ./tools/dev/run_dev_server.sh local-offload-s3

#shell3 (weaviate1)
OFFLOAD_S3_ENDPOINT="http://localhost:9000" OFFLOAD_S3_BUCKET_AUTO_CREATE="true" ./tools/dev/run_dev_server.sh second-offload-s3

#shell4 (weaviate2)
OFFLOAD_S3_ENDPOINT="http://localhost:9000" OFFLOAD_S3_BUCKET_AUTO_CREATE="true" ./tools/dev/run_dev_server.sh third-offload-s3

#shell5 (querier)
go build ./cmd/weaviate && OFFLOAD_S3_ENDPOINT="http://localhost:9000" AWS_ACCESS_KEY_ID="aws_access_key" AWS_SECRET_KEY="aws_secret_key" ./weaviate --target=querier --query.s3.endpoint http://localhost:9000
```

Create a class/tenant and insert some data, for example using this python script (need to pip install `weaviate-client`):

```python
import weaviate
import weaviate.classes.config as wvcc
import weaviate.collections.classes.tenants as t

client = weaviate.connect_to_local()

client.collections.delete_all()

collection_name = "Question"
tenant_name = "kavi"
collection = client.collections.create(
    name=collection_name,
    properties=[
        wvcc.Property(
            name="title",
            data_type=wvcc.DataType.TEXT
        )
    ],
    multi_tenancy_config=wvcc.Configure.multi_tenancy(enabled=True),
    replication_config=wvcc.Configure.replication(factor=3),
    vector_index_config=wvcc.Configure.VectorIndex.flat(
        quantizer=wvcc.Configure.VectorIndex.Quantizer.bq()
    ),
)
tenant = t.Tenant(name=tenant_name, activity_status=t.TenantCreateActivityStatus.ACTIVE)
collection.tenants.create(tenant)
collection_tenant = collection.with_tenant(tenant)
uuid = collection_tenant.data.insert(
    properties={
        "title": "foo",
    },
    vector=[0.1, 0.2],
)
tenant_offloaded = t.Tenant(name=tenant_name, activity_status=t.TenantUpdateActivityStatus.OFFLOADED)
collection.tenants.update(tenant_offloaded)

client.close()
```

Submit a grpc search to the querier, you should see 1 result.

```sh
grpcurl -plaintext -d '{"collection": "'"Question"'", "tenant": "kavi", "limit": 10}' localhost:9090 weaviate.v1.Weaviate.Search
```

You can see the downloaded tenant files:

```sh
ls -l /tmp/question/kavi
```

insert some more data:

```py
import time
import weaviate
import weaviate.collections.classes.tenants as t

client = weaviate.connect_to_local()

collection_name = "Question"
tenant_name = "kavi"
collection = client.collections.get(collection_name)
tenant = t.Tenant(name=tenant_name, activity_status=t.TenantCreateActivityStatus.ACTIVE)
collection.tenants.update(tenant)
time.sleep(1)
collection_tenant = collection.with_tenant(tenant)
uuid = collection_tenant.data.insert(
    properties={
        "title": "foo",
    },
    vector=[0.1, 0.2],
)

client.close()
```

Swap the tenant between ACTIVE/OFFLOADED:

```py
import time
import weaviate
import weaviate.collections.classes.tenants as t

client = weaviate.connect_to_local()

collection = client.collections.get("Question")

for i in range(1):
    t1_active = t.Tenant(name=f'kavi', activity_status=t.TenantUpdateActivityStatus.ACTIVE)
    collection.tenants.update(t1_active)
    time.sleep(1)
    t1_offloaded = t.Tenant(name=f'kavi', activity_status=t.TenantUpdateActivityStatus.OFFLOADED)
    collection.tenants.update(t1_offloaded)
    time.sleep(1)

client.close()
```

The tenant files should be downloaded asynchronously onto the querier each time the tenant is offloaded. Rerun the query to see the new results.
