# Authorization Calls
This document lists all authorization calls in the codebase.
## Usage
To regenerate this documentation, run the following commands from the repository root:
```bash
cd usecases/auth/authorization/docs
go run generator.go
```
## Statistics
- Total files found: 23162
- Files processed: 1937
- Total Authorize calls found: 79

| Function | File | Verb → Resources |
|----------|------|-----------------|
| resolveAggregate | adapters/handlers/graphql/local/aggregate/resolver.go | READ → ShardsData |
| authorizePath | adapters/handlers/graphql/local/common_filters/authz.go | READ → CollectionsData |
| AuthorizeProperty | adapters/handlers/graphql/local/common_filters/authz.go | READ → CollectionsData |
| resolveExplore | adapters/handlers/graphql/local/explore/concepts_resolver.go | READ → CollectionsData |
| resolveGet | adapters/handlers/graphql/local/get/class_builder_fields.go | READ → ShardsData |
| batchDelete | adapters/handlers/grpc/v1/service.go | DELETE → ShardsData |
| batchObjects | adapters/handlers/grpc/v1/service.go | UPDATE → ShardsData<br>CREATE → ShardsData |
| classGetterWithAuthzFunc | adapters/handlers/grpc/v1/service.go | READ → Collections |
| createRole | adapters/handlers/rest/authz/handlers_authz.go | CREATE → Roles |
| addPermissions | adapters/handlers/rest/authz/handlers_authz.go | UPDATE → Roles |
| removePermissions | adapters/handlers/rest/authz/handlers_authz.go | UPDATE → Roles |
| hasPermission | adapters/handlers/rest/authz/handlers_authz.go | READ → Roles |
| getRoles | adapters/handlers/rest/authz/handlers_authz.go | READ → Roles |
| getRole | adapters/handlers/rest/authz/handlers_authz.go | READ → Roles |
| deleteRole | adapters/handlers/rest/authz/handlers_authz.go | DELETE → Roles |
| assignRole | adapters/handlers/rest/authz/handlers_authz.go | UPDATE → Roles |
| getRolesForUser | adapters/handlers/rest/authz/handlers_authz.go | READ → Roles |
| getUsersForRole | adapters/handlers/rest/authz/handlers_authz.go | READ → Roles |
| revokeRole | adapters/handlers/rest/authz/handlers_authz.go | UPDATE → Roles |
| setupGraphQLHandlers | adapters/handlers/rest/handlers_graphql.go | READ → CollectionsMetadata, Collections |
| Backup | usecases/backup/scheduler.go | CREATE → Backups |
| Restore | usecases/backup/scheduler.go | CREATE → Backups |
| Cancel | usecases/backup/scheduler.go | DELETE → Backups |
| Schedule | usecases/classification/classifier.go | UPDATE → CollectionsMetadata |
| validateFilter | usecases/classification/classifier.go | READ → CollectionsMetadata |
| Get | usecases/classification/classifier.go | READ → CollectionsMetadata |
| GetNodeStatus | usecases/nodes/handler.go | READ → Nodes |
| GetNodeStatistics | usecases/nodes/handler.go | READ → Cluster |
| AddObject | usecases/objects/add.go | CREATE → ShardsData<br>READ → CollectionsMetadata |
| autoSchema | usecases/objects/auto_schema.go | CREATE → CollectionsMetadata<br>UPDATE → CollectionsMetadata |
| AddObjects | usecases/objects/batch_add.go | UPDATE → ShardsData<br>CREATE → ShardsData |
| DeleteObjects | usecases/objects/batch_delete.go | DELETE → ShardsData |
| classGetterFunc | usecases/objects/batch_delete.go | READ → Collections |
| AddReferences | usecases/objects/batch_references_add.go | UPDATE → pathsData<br>READ → pathsMetadata |
| addReferences | usecases/objects/batch_references_add.go | READ → shardsDataPaths |
| DeleteObject | usecases/objects/delete.go | READ → CollectionsMetadata<br>DELETE → Objects |
| GetObject | usecases/objects/get.go | READ → Objects |
| GetObjects | usecases/objects/get.go | READ → Objects |
| GetObjectsClass | usecases/objects/get.go | READ → Objects |
| HeadObject | usecases/objects/head.go | READ → Objects, CollectionsMetadata |
| MergeObject | usecases/objects/merge.go | UPDATE → Objects<br>READ → CollectionsMetadata |
| Query | usecases/objects/query.go | READ → CollectionsMetadata |
| AddObjectReference | usecases/objects/references_add.go | UPDATE → ShardsData<br>READ → CollectionsMetadata, Collections |
| DeleteObjectReference | usecases/objects/references_delete.go | UPDATE → ShardsData<br>READ → CollectionsMetadata, CollectionsData |
| UpdateObjectReferences | usecases/objects/references_update.go | UPDATE → ShardsData<br>READ → CollectionsMetadata, Collections |
| UpdateObject | usecases/objects/update.go | UPDATE → Objects<br>READ → CollectionsMetadata |
| ValidateObject | usecases/objects/validate.go | READ → Objects |
| GetClass | usecases/schema/class.go | READ → CollectionsMetadata |
| GetConsistentClass | usecases/schema/class.go | READ → CollectionsMetadata |
| GetCachedClass | usecases/schema/class.go | READ → CollectionsMetadata |
| AddClass | usecases/schema/class.go | CREATE → CollectionsMetadata<br>READ → CollectionsMetadata |
| DeleteClass | usecases/schema/class.go | DELETE → CollectionsMetadata<br>READ → CollectionsMetadata |
| UpdateClass | usecases/schema/class.go | UPDATE → CollectionsMetadata |
| GetSchema | usecases/schema/handler.go | READ → CollectionsMetadata |
| GetConsistentSchema | usecases/schema/handler.go | READ → CollectionsMetadata |
| UpdateShardStatus | usecases/schema/handler.go | UPDATE → ShardsMetadata |
| ShardsStatus | usecases/schema/handler.go | READ → ShardsMetadata |
| AddClassProperty | usecases/schema/property.go | UPDATE → CollectionsMetadata<br>READ → CollectionsMetadata |
| DeleteClassProperty | usecases/schema/property.go | UPDATE → CollectionsMetadata |
| AddTenants | usecases/schema/tenant.go | CREATE → ShardsMetadata |
| UpdateTenants | usecases/schema/tenant.go | UPDATE → ShardsMetadata |
| DeleteTenants | usecases/schema/tenant.go | DELETE → ShardsMetadata |
| GetConsistentTenants | usecases/schema/tenant.go | READ → ShardsMetadata |
| ConsistentTenantExists | usecases/schema/tenant.go | READ → ShardsMetadata |
| validateFilters | usecases/traverser/traverser_get.go | READ → CollectionsMetadata |
