import weaviate.classes as wvc
from .conftest import CollectionFactory


def test_tenant_activation(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=True)
    )

    tenant_names = ["tenant1", "tenant2"]
    collection.tenants.create(tenants=tenant_names)

    tenants = collection.tenants.get()
    for name, tenant in tenants.items():
        assert name in tenant_names
        assert tenant.activity_status == wvc.tenants.TenantActivityStatus.ACTIVE

    collection.tenants.update(
        tenants=[
            wvc.tenants.TenantUpdate(
                name=name, activity_status=wvc.tenants.TenantUpdateActivityStatus.INACTIVE
            )
            for name in tenant_names
        ]
    )
    tenants = collection.tenants.get()
    for name, tenant in tenants.items():
        assert name in tenant_names
        assert tenant.activity_status == wvc.tenants.TenantActivityStatus.INACTIVE
