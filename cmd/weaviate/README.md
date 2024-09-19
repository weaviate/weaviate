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
import weaviate
import weaviate.classes.config as wvcc
import weaviate.collections.classes.tenants as t

client = weaviate.connect_to_local()

collection_name = "Question"
tenant_name = "kavi"
collection = client.collections.get(collection_name)
tenant = t.Tenant(name=tenant_name, activity_status=t.TenantCreateActivityStatus.ACTIVE)
collection.tenants.update(tenant)

collection_tenant = collection.with_tenant(tenant)
uuid = collection_tenant.data.insert(
    properties={
        "title": "foo",
    },
    vector=[0.1, 0.2],
)

client.close()
```

Swap the tenant between ACTIVE/OFFLOADED a few times:

```py
import time
import weaviate
import weaviate.collections.classes.tenants as t

client = weaviate.connect_to_local()

collection = client.collections.get("Question")

for i in range(3):
    t1_active = t.Tenant(name=f'kavi', activity_status=t.TenantUpdateActivityStatus.ACTIVE)
    collection.tenants.update(t1_active)
    time.sleep(1)
    t1_offloaded = t.Tenant(name=f'kavi', activity_status=t.TenantUpdateActivityStatus.OFFLOADED)
    collection.tenants.update(t1_offloaded)
    time.sleep(1)

client.close()
```

The tenant files should be downloaded asynchronously onto the querier each time the tenant is offloaded. Rerun the query to see the new results.
