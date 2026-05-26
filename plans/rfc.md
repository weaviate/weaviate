## Summary

Stage 1 namespace graduation reuses the existing collection backup/restore path for collection schema, sharding state, aliases, and shard files. It does not need a new shard artefact format, a new placement subsystem, or general shard redistribution logic.

The DB changes are limited to three areas. First, `NodeMapping` restore becomes an explicit stage-1 contract for one-to-one source-node to target-node remaps with unchanged replication factor. Second, backup supports namespace-scoped RBAC and dynamic-user filtering so graduation moves only namespace-bound users and roles. Third, restore supports target-specific materialization. For stage 1, that means namespace stripping because the target cluster is namespace-disabled. Later, the same backup can also restore unchanged into a namespace-enabled target, with any replication scale-out handled after restore.

Graduation must also use transfer storage that is separate from the daily backup buckets. The upload/download mechanics can stay the same, but the bucket/path, lifecycle, and credentials for graduation runs must be isolated from routine backup policy. Nothing needs to change from the PoV of the database itself, but the operator must have a separate workflow for graduation backup/restore that does not interfere with disaster recovery.

## Background (Problem Statement)

Today, backup/restore is collection-scoped. The scheduler resolves an include/exclude class list and coordinates backup and restore from that final set. This is already close to what namespace graduation needs because namespaced collections are stored as namespace-qualified collection names. If the caller can enumerate the collections owned by a namespace, backup/restore can already move their data.

Restore already has the right hook for node-name translation. `BackupRequest` carries `NodeMapping`, the scheduler copies it onto the distributed descriptor, the coordinator uses the mapped node set to decide which nodes participate, and class restore rewrites the stored `sharding.State` before restoring the collection through RAFT. That is enough for the stage-1 case where an RF=1 namespaced collection is restored onto a single mapped node in the target cluster. Any later scale-out to RF=3 happens after restore.

The remaining gaps are metadata and naming. The backup descriptor carries RBAC and dynamic-user state as opaque whole-cluster blobs. Restore either replays those blobs or skips them via `rolesOptions=noRestore|all` and `usersOptions=noRestore|all`. That behaviour is correct for disaster recovery. It is too coarse for namespace graduation, which needs to move only one namespace's control-plane state.

There is also a target-shape problem. The source cluster is namespace-enabled, but graduation targets may not all want the same result. A namespace-disabled target needs stripped names. A namespace-enabled target may want the names preserved. Graduation splits responsibilities cleanly:

1. backup selects the namespace-scoped data,
2. restore decides how to materialize that data on the target.

## Proposal

Stage 1 graduation is implemented as:

1. backup and restore namespaced collection data, e.g. `namespace:collection`, through the existing backup/restore path,
2. backup and restore namespaced metadata, e.g. `namespace:role` and `namespace:user`, through extensions to the existing backup/restore path,
3. keep the backup source-faithful and let restore decide how to materialize it on the target,
4. for stage 1, strip namespace qualifiers during restore, e.g. `namespace:Collection` -> `Collection`, `namespace:role` -> `role`, `namespace:user` -> `user`,
5. apply a one-to-one `NodeMapping` during restore, e.g. `{"node5": "node0"}` for an RF=1 source shard.

## Implementation Details

### Collection backup and restore

The collection data plane stays collection-scoped:

1. the caller resolves the namespace to a list of concrete collection names,
2. backup runs with that explicit include-list,
3. backup keeps the source collection and alias names unchanged in the artefact,
4. restore replays those collections through the existing path,
5. restore applies the target materialization rule.

No change is required to shard snapshotting, file upload, `ClassDescriptor`, or the shard-file format. This already includes aliases. Aliases are serialised into each `ClassDescriptor` during backup and replayed during class restore. The backup artefact retains the source names as captured. That keeps the artefact stable while allowing restore to choose the target naming model.

For stage 1, the target cluster is namespace-disabled and empty. Restore strips the source namespace prefix before creating collections and aliases.

### Target-specific materialization on restore

The key contract is:

1. backup is responsible for namespace-scoped selection,
2. restore is responsible for target-specific materialization.

For stage 1, the materialization rule is namespace stripping. That rule is scoped to the namespace being graduated and strips only that namespace prefix during restore.

The stage-1 transformation rules are:

1. collection names restore from `namespace:ClassName` to `ClassName`,
2. alias names restore under the same stripping rule when they are namespace-qualified,
3. dynamic users restore from namespaced identities to bare user ids and have their stored namespace cleared,
4. RBAC roles restore from `namespace:role` to `role`, and grouping-policy assignments are rewritten to the stripped role and subject identifiers for the migrated namespace-scoped principals.

This keeps the backup generic. For a namespace-disabled target, restore strips the names. For a future namespace-enabled target, restore can preserve the names instead. If that future target also wants a higher replication factor, it can scale after restore. Ordinary restore continues to replay the names stored in the backup unchanged.

### Translation of replica topologies

Backup/restore can only rename a source node to one target node. `NodeMapping` is a one-to-one rename map, not a placement or scaling plan.

For an RF=1 source collection, the supported shape is:

```json
{
  "node5": "node0"
}
```

for a collection that lived on `node5` in the source cluster and is restored onto `node0` in the target cluster.

The important limit is that backup/restore cannot fan one source node out to several target nodes. A shape such as:

```json
{
  "node5": ["node0", "node1", "node2"]
}
```

is not supported. Restore can only land the restored shard on a single target node for that source node.

That means the backup/restore contract has three parts:

1. the mapping is complete and one-to-one,
2. restore preserves the existing replica count exactly,
3. the shard placement must already fit the target cluster after the rename step.

Within those constraints, the existing restore path is correct. Outside them, backup/restore fails preflight instead of attempting redistribution or scale-out.

For the RF=1 -> RF=3 graduation case, the workflow must therefore be split into two journeys:

1. restore the collection with a one-to-one node mapping, e.g. `{"node5": "node0"}`, which keeps it non-HA after restore,
2. run a separate replica-movement scale-out journey to raise replication from 1 to 3.

The existing v1 replication APIs are the right surface for that second journey. At a high level the flow is:

1. get a scale plan with `GET /v1/replication/scale`,
2. apply that plan with `POST /v1/replication/scale`,
3. monitor the resulting replica-movement operations via `GET /v1/replication/replicate/{id}`,
4. optionally verify the final shard layout via `GET /v1/replication/sharding-state`.

The RFC records that hand-off, but it does not pull the replica-movement orchestration into the backup/restore contract itself. The backup/restore part ends once the collection has been restored onto its single mapped target node.

There is one concrete code fix required here: `DistributedBackupDescriptor.ApplyNodeMapping` currently does not rewrite an existing node entry when the source key is present because the condition is inverted. Graduation depends on remap being authoritative, so that bug must be fixed before the workflow ships.

### Namespace metadata backup/restore

Stage 1 extends the existing backup/restore API instead of adding a second metadata API. In addition to the collection include-list, backup/restore accepts namespace-scoped metadata selectors such as `"namespace:role"` and `"namespace:user"`. These selectors are request inputs, not an instruction for operators to edit snapshot bytes by hand.

For dynamic users, `includeUsers: ["namespace:*"]` means "include all dynamic DB users whose stored `User.Namespace` equals `namespace`". That is the right level to filter because the DB already owns the credential material that must be preserved during restore. The backup keeps those source identities unchanged. Restore then materializes them for the target. In stage 1, that means restoring them as unnamespaced users.

For RBAC, `includeRoles: ["namespace:*"]` means "include all roles that belong to namespace `namespace`". Roles are namespace-scoped in the intended model, so the selector follows the same namespace-qualified pattern as collections and users. The backup keeps those source role names unchanged. Restore then materializes them for the target. In stage 1, that means stripping the role names before applying them to the target cluster.

For stage 1, the RBAC filter rule is conservative:

1. back up roles selected by the namespace-scoped include-list,
2. back up grouping-policy assignments only for backed-up roles and migrated subjects.

### Metadata payload and restore semantics

The filtered metadata still travels through the existing backup descriptor. When `includeUsers` or `includeRoles` is supplied, the corresponding `UserBackups` and `RbacBackups` payloads are namespace-filtered snapshots, not whole-cluster snapshots. Restore applies the chosen materialization rule before loading them into the target cluster. In stage 1, that means rewriting the selected user and role identifiers by stripping the namespace. `includeUsers` and `includeRoles` are mutually exclusive with `usersOptions` and `rolesOptions` respectively.

Stage 1 requires an empty target cluster. Under that assumption, the current restore semantics are acceptable:

1. RBAC restore currently clears all `casbin` policy before loading the supplied snapshot,
2. DB-user restore currently replaces the stored user state with the supplied snapshot.

That behaviour is correct for stage 1 because the target cluster is empty. Graduation does **not** need merge semantics or collision handling in this stage. It needs source-side filtering, then target-side restore behavior that materializes and applies the filtered snapshot.

### Graduation contract

The DB contract exposed to an operator, runbook, or automation is:

1. resolve namespace -> collection set,
2. choose the target materialization rule,
3. verify the collection set satisfies the one-to-one remap constraint,
4. back up those collections and the filtered namespace metadata (roles and users) into graduation transfer storage,
5. restore onto the target with the chosen restore rule and `NodeMapping`,
6. require that the target cluster is empty,
7. run parity checks against the same namespace collection set, accounting for the target names produced by that rule.
8. hand off to a separate replica-movement scale-out journey in order to raise replication from 1 to 3.

That contract is enough for stage 1. It does not require the DB to solve live catch-up, online cutover, or arbitrary topology reshaping.

## **Backwards compatibility**

This work is additive to ordinary backup/restore.

1. Disaster-recovery backup/restore continues to support whole-cluster RBAC and dynamic-user snapshots.
2. Collection backup artefacts and shard-file formats remain unchanged.
3. `NodeMapping` restore remains the placement mechanism, but graduation narrows the supported shape to complete one-to-one remaps with unchanged replication factor.
4. Namespace-scoped metadata movement extends backup/restore with filtered selectors such as `includeUsers` and `includeRoles`.
5. Target-specific materialization happens at restore time. Stage 1 uses namespace stripping for a namespace-disabled target. A future namespace-enabled target can preserve the names instead, and handle any replication scale-out after restore. Aliases remain part of collection backup/restore.

Stage 1 is therefore backwards compatible with existing backup artefacts and restore flows while introducing a graduation-specific restore contract that can support more than one target shape.