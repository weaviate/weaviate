# Authorization Calls
This document lists all authorization calls in the codebase.
## Usage
To regenerate this documentation, run the following commands from the repository root:
```bash
cd usecases/auth/authorization/docs
go run generator.go
```
## Statistics
- Total files found: 6070
- Files processed: 2925
- Total Authorize calls found: 112

| Function | File | Verb → Resources |
|----------|------|-----------------|
| resolveAggregate | adapters/handlers/graphql/local/aggregate/resolver.go | principal → READ |
| authorizePath | adapters/handlers/graphql/local/common_filters/authz.go | principal → READ |
| AuthorizeProperty | adapters/handlers/graphql/local/common_filters/authz.go | principal → READ |
| resolveExplore | adapters/handlers/graphql/local/explore/concepts_resolver.go | principal → READ |
| resolveGet | adapters/handlers/graphql/local/get/class_builder_fields.go | principal → READ |
| BatchObjects | adapters/handlers/grpc/v1/batch/handler.go | principal → UPDATE, CREATE |
| batchDelete | adapters/handlers/grpc/v1/service.go | principal → DELETE |
| classGetterWithAuthzFunc | adapters/handlers/grpc/v1/service.go | principal → READ |
| Authorize | adapters/handlers/mcp/auth/auth.go | principal → verb |
| AuthorizeCollectionData | adapters/handlers/mcp/auth/auth.go | principal → verb |
| UpsertObject | adapters/handlers/mcp/create/objects_upsert.go | req → CREATE, UPDATE |
| GetCollectionConfig | adapters/handlers/mcp/read/collections.go | req → READ |
| GetTenants | adapters/handlers/mcp/read/tenants.go | req → READ |
| Hybrid | adapters/handlers/mcp/search/hybrid.go | req → READ |
| authorizeRoleScopes | adapters/handlers/rest/authz/handlers_authz.go | principal → VerbWithScope |
| assignRoleToUser | adapters/handlers/rest/authz/handlers_authz.go | principal → USER_AND_GROUP_ASSIGN_AND_REVOKE |
| assignRoleToGroup | adapters/handlers/rest/authz/handlers_authz.go | principal → USER_AND_GROUP_ASSIGN_AND_REVOKE |
| getRolesForUserDeprecated | adapters/handlers/rest/authz/handlers_authz.go | principal → READ |
| getRolesForUser | adapters/handlers/rest/authz/handlers_authz.go | principal → READ |
| revokeRoleFromUser | adapters/handlers/rest/authz/handlers_authz.go | principal → USER_AND_GROUP_ASSIGN_AND_REVOKE |
| revokeRoleFromGroup | adapters/handlers/rest/authz/handlers_authz.go | principal → USER_AND_GROUP_ASSIGN_AND_REVOKE |
| getRolesForGroup | adapters/handlers/rest/authz/handlers_authz.go | principal → READ |
| getUser | adapters/handlers/rest/db_users/handlers_db_users.go | principal → READ |
| createUser | adapters/handlers/rest/db_users/handlers_db_users.go | principal → CREATE |
| rotateKey | adapters/handlers/rest/db_users/handlers_db_users.go | principal → UPDATE |
| deleteUser | adapters/handlers/rest/db_users/handlers_db_users.go | principal → DELETE |
| deactivateUser | adapters/handlers/rest/db_users/handlers_db_users.go | principal → UPDATE |
| activateUser | adapters/handlers/rest/db_users/handlers_db_users.go | principal → UPDATE |
| setupGraphQLHandlers | adapters/handlers/rest/handlers_graphql.go | principal → READ |
| getIndexes | adapters/handlers/rest/handlers_indexes.go | principal → READ |
| qualifyAndAuthorize | adapters/handlers/rest/handlers_indexes_upsert.go | principal → UPDATE |
| deleteClassPropertyIndex | adapters/handlers/rest/handlers_schema.go | principal → UPDATE |
| propertyTokenize | adapters/handlers/rest/handlers_tokenize.go | principal → READ |
| createNamespace | adapters/handlers/rest/namespaces/handlers_namespaces.go | principal → CREATE |
| updateNamespace | adapters/handlers/rest/namespaces/handlers_namespaces.go | principal → UPDATE |
| getNamespace | adapters/handlers/rest/namespaces/handlers_namespaces.go | principal → READ |
| deleteNamespace | adapters/handlers/rest/namespaces/handlers_namespaces.go | principal → DELETE |
| changeState | adapters/handlers/rest/namespaces/handlers_namespaces.go | principal → UPDATE |
| replicate | adapters/handlers/rest/replication/handlers_replicate.go | principal → CREATE |
| getReplicationDetailsByReplicationId | adapters/handlers/rest/replication/handlers_replicate.go | principal → READ |
| deleteReplication | adapters/handlers/rest/replication/handlers_replicate.go | principal → DELETE |
| deleteAllReplications | adapters/handlers/rest/replication/handlers_replicate.go | principal → DELETE |
| forceDeleteReplications | adapters/handlers/rest/replication/handlers_replicate.go | principal → DELETE |
| cancelReplication | adapters/handlers/rest/replication/handlers_replicate.go | principal → UPDATE |
| listReplication | adapters/handlers/rest/replication/handlers_replicate.go | principal → READ |
| getCollectionShardingState | adapters/handlers/rest/replication/handlers_replicate.go | principal → READ |
| getReplicationScalePlan | adapters/handlers/rest/replication/handlers_replicate.go | principal → READ |
| applyReplicationScalePlan | adapters/handlers/rest/replication/handlers_replicate.go | principal → UPDATE |
| hideAliasTarget | adapters/handlers/rest/search/handler.go | principal → READ |
| classGetterWithAuthz | adapters/handlers/rest/search/handler.go | principal → READ |
| AuthorizeSilent | usecases/auth/authorization/adminlist/authorizer.go | principal → verb |
| FilterAuthorizedResources | usecases/auth/authorization/adminlist/authorizer.go | principal → verb |
| Filter | usecases/auth/authorization/filter/filter.go | principal → verb |
| AuthorizeSilent | usecases/auth/authorization/mocks/authorizer.go | principal → verb |
| FilterAuthorizedResources | usecases/auth/authorization/mocks/authorizer.go | principal → verb |
| RolePoliciesVisibleToPrincipal | usecases/auth/authorization/rolevisibility/rolevisibility.go | principal → VerbWithScope |
| Backup | usecases/backup/scheduler.go | pr → CREATE |
| Restore | usecases/backup/scheduler.go | pr → CREATE |
| filterBackupableClasses | usecases/backup/scheduler.go | pr → verb |
| authorizeBackupByID | usecases/backup/scheduler.go | principal → verb |
| Cancel | usecases/backup/scheduler.go | principal → DELETE |
| CancelRestore | usecases/backup/scheduler.go | principal → DELETE |
| List | usecases/backup/scheduler.go | principal → READ |
| classGetterWithAuthzFunc | usecases/classification/classifier.go | principal → READ |
| Schedule | usecases/classification/classifier.go | principal → UPDATE |
| validateFilter | usecases/classification/classifier.go | principal → READ |
| Get | usecases/classification/classifier.go | principal → READ |
| ListTasks | usecases/distributedtask/handler.go | principal → READ |
| Status | usecases/export/scheduler.go | principal → READ |
| Cancel | usecases/export/scheduler.go | principal → DELETE |
| GetNodeStatus | usecases/nodes/handler.go | principal → READ |
| GetNodeStatistics | usecases/nodes/handler.go | principal → READ |
| AddObject | usecases/objects/add.go | principal → CREATE |
| AddObjects | usecases/objects/batch_add.go | principal → UPDATE, CREATE |
| DeleteObjects | usecases/objects/batch_delete.go | principal → DELETE |
| classGetterFunc | usecases/objects/batch_delete.go | principal → READ |
| AddReferences | usecases/objects/batch_references_add.go | principal → UPDATE |
| addReferences | usecases/objects/batch_references_add.go | principal → READ |
| DeleteObject | usecases/objects/delete.go | principal → DELETE |
| GetObject | usecases/objects/get.go | principal → READ |
| GetObjects | usecases/objects/get.go | principal → READ |
| GetObjectsClass | usecases/objects/get.go | principal → READ |
| HeadObject | usecases/objects/head.go | principal → READ |
| MergeObject | usecases/objects/merge.go | principal → UPDATE |
| Query | usecases/objects/query.go | principal → READ |
| AddObjectReference | usecases/objects/references_add.go | principal → UPDATE, READ |
| DeleteObjectReference | usecases/objects/references_delete.go | principal → READ, UPDATE |
| UpdateObjectReferences | usecases/objects/references_update.go | principal → UPDATE, READ |
| UpdateObject | usecases/objects/update.go | principal → UPDATE |
| ValidateObject | usecases/objects/validate.go | principal → READ |
| GetAlias | usecases/schema/alias.go | principal → READ |
| AddAlias | usecases/schema/alias.go | principal → CREATE |
| UpdateAlias | usecases/schema/alias.go | principal → UPDATE |
| DeleteAlias | usecases/schema/alias.go | principal → DELETE |
| GetClass | usecases/schema/class.go | principal → READ |
| GetConsistentClass | usecases/schema/class.go | principal → READ |
| GetCachedClass | usecases/schema/class.go | principal → READ |
| AddClass | usecases/schema/class.go | principal → CREATE, DELETE, READ |
| DeleteClass | usecases/schema/class.go | principal → DELETE |
| UpdateClass | usecases/schema/class.go | principal → UPDATE, DELETE |
| UpdateShardStatus | usecases/schema/handler.go | principal → UPDATE |
| ShardsStatus | usecases/schema/handler.go | principal → READ |
| AddClassProperty | usecases/schema/property.go | principal → UPDATE, READ |
| DeleteClassPropertyIndex | usecases/schema/property.go | principal → UPDATE |
| DeleteClassVectorIndex | usecases/schema/property.go | principal → UPDATE |
| DeleteClassProperty | usecases/schema/property.go | principal → UPDATE |
| AddTenants | usecases/schema/tenant.go | principal → CREATE |
| UpdateTenants | usecases/schema/tenant.go | principal → UPDATE |
| DeleteTenants | usecases/schema/tenant.go | principal → DELETE |
| GetConsistentTenant | usecases/schema/tenant.go | principal → READ |
| ConsistentTenantExists | usecases/schema/tenant.go | principal → READ |
| validateFilters | usecases/traverser/traverser_get.go | principal → READ |
