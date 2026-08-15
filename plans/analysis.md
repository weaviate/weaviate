# Deep Analysis: UpdateObject vs MergeObject - Object Fetch Dependencies

## EXECUTIVE SUMMARY

Both UpdateObject and MergeObject fetch the existing object from the local shard BEFORE the write. This fetch is used for:
1. **Validation** (tenant mismatch check, cross-reference validation)
2. **Vectorization comparison** (reVectorize check - compares old vs new properties)
3. **Metadata preservation** (creation time for updates, default vector preservation for merges)
4. **Dirty-read detection** (for deleted objects during replication)

The vectorization step is the primary bottleneck and legitimate reason for the fetch, but only for **comparison purposes** - not because new vectors cannot be generated without the old object.

---

## UpdateObject Flow (update.go, lines 63-130)

### Pre-Write Operations That Depend on Existing Object:

```
Line 75:  obj, err := m.getObjectFromRepo(ctx, className, id, additional.Properties{}, repl, updates.Tenant)
          ↓ Fetches full object from vectorRepo

Line 98:  prevObj := obj.Object()
          ↓ Extracts object

Line 99:  m.validateObjectAndNormalizeNames(ctx, repl, updates, prevObj, fetchedClasses)
          ├─ Uses prevObj ONLY for tenant mismatch check (properties_validation.go:50-51)
          └─ Uses prevObj in cRef validation to validate reference existence (but does NOT examine prevObj content)

Line 108: updates.CreationTimeUnix = obj.Created
          └─ MUST preserve original creation time

Line 111: m.modulesProvider.UpdateVector(ctx, updates, class, m.findObject, m.logger)
          └─ **CRITICAL**: Uses m.findObject callback which calls vectorRepo.Object() internally
             when reVectorize check is needed (details below)

Line 124: m.vectorRepo.PutObject(...)
          └─ Writes through RAFT
```

### Detailed Analysis of UpdateVector Flow:

**vectorizer.go, vectorizeOne() (lines 341-356):**
- Calls `shouldVectorize()` to determine if re-vectorization is needed
- If yes: calls `vectorize()` which calls the actual vectorizer module
- If no: skips vectorization (uses existing vectors)

**vectorizer.go, vectorize() (lines 358-441):**
- Calls `reVectorize()` (compare.go:27-55) with the **provided object**
- `reVectorize()` is a COMPARISON function, not a generation function

**compare.go, reVectorize() (lines 27-55):**
```go
func reVectorize(ctx context.Context,
    cfg moduletools.ClassConfig,
    mod modulecapabilities.Vectorizer[[]float32],
    object *models.Object,  // <-- THE PROVIDED UPDATED OBJECT
    class *models.Class,
    sourceProperties []string,
    targetVector string,
    findObjectFn modulecapabilities.FindObjectFn,  // <-- CB to fetch old object IF needed
    reVectorizeDisabled bool,
) (bool, models.AdditionalProperties, []float32, error) {
    if reVectorizeDisabled {
        return true, nil, nil, nil
    }

    // Call reVectorizeEmbeddings which MAY call findObjectFn
    shouldReVectorize, oldObject := reVectorizeEmbeddings(ctx, cfg, mod, object, class, sourceProperties, findObjectFn)
    
    if shouldReVectorize {  // <-- If re-vectorization needed, return true
        return shouldReVectorize, nil, nil, nil
    }

    // If NOT re-vectorizing, return OLD object's vectors
    if targetVector == "" {
        return false, oldObject.AdditionalProperties, oldObject.Vector, nil
    } else {
        return false, oldObject.AdditionalProperties, vector, nil
    }
}
```

**compare.go, reVectorizeEmbeddings() (lines 109-237):**
```go
func reVectorizeEmbeddings[T dto.Embedding](ctx context.Context,
    cfg moduletools.ClassConfig,
    mod modulecapabilities.Vectorizer[T],
    object *models.Object,  // <-- PROVIDED UPDATED OBJECT
    class *models.Class,
    sourceProperties []string,
    findObjectFn modulecapabilities.FindObjectFn,
) (bool, *search.Result) {
    // ... determine which properties are vectorizable ...
    
    // Line 178-182: If no properties to compare, fetch old object
    if len(propsToCompare) == 0 {
        oldObject, err := findObjectFn(ctx, class.Class, object.ID, nil, additional.Properties{}, object.Tenant)
        if err != nil || oldObject == nil {
            return true, nil  // Force re-vectorization
        }
        return false, oldObject  // Return old vectors (no properties changed)
    }
    
    // Line 189: Fetch OLD object with ONLY the vectorizable properties
    oldObject, err := findObjectFn(ctx, class.Class, object.ID, returnProps, additional.Properties{}, object.Tenant)
    if err != nil || oldObject == nil {
        return true, nil  // Force re-vectorization
    }
    
    // Line 193-235: COMPARE old vs new property values
    oldProps := oldObject.Schema.(map[string]interface{})
    newProps := object.Properties.(map[string]interface{})
    for _, propStruct := range propsToCompare {
        valNew, isPresentNew := newProps[propStruct.Name]
        valOld, isPresentOld := oldProps[propStruct.Name]
        
        // Property presence changed?
        if isPresentNew != isPresentOld {
            return true, nil  // Force re-vectorization
        }
        
        // Property value changed?
        if valOld != valNew {
            return true, nil  // Force re-vectorization
        }
    }
    
    return false, oldObject  // No vectorizable properties changed
}
```

**KEY INSIGHT**: The `findObjectFn` is called CONDITIONALLY within `reVectorizeEmbeddings()` ONLY IF:
1. There are vectorizable properties in the schema, AND
2. The system needs to compare old vs new property values

The comparison is done to optimize: "Do the vectorizable properties actually change?"

---

## MergeObject Flow (merge.go, lines 45-127)

### Pre-Write Operations That Depend on Existing Object:

```
Line 79:  obj, err := m.vectorRepo.Object(ctx, cls, id, nil, additional.Properties{}, repl, updates.Tenant)
          ↓ Fetches full object

Line 113: prevObj := obj.Object()
          ↓ Extracts object

Line 114: m.validateObjectAndNormalizeNames(ctx, repl, updates, prevObj, fetchedClass)
          └─ Same as Update: tenant check + cRef validation

Line 122: m.patchObject(ctx, prevObj, updates, repl, ...)
          └─ Calls mergeObjectSchemaAndVectorize which:
             ├─ Merges prevObj.Properties with updates.Properties (line 136-141)
             └─ Calls UpdateVector(ctx, mergedObject, class, m.findObject, logger) (line 234)
             └─ Like Update: vectorization with conditional old-object fetch
             └─ Preserves previous vectors if vectorizer is "none" (lines 239-264)
```

### patchObject -> mergeObjectSchemaAndVectorize (lines 206-267):

```go
func (m *Manager) mergeObjectSchemaAndVectorize(ctx context.Context, 
    prevPropsSch models.PropertySchema,
    nextProps map[string]interface{}, 
    prevVec, nextVec []float32, 
    prevVecs models.Vectors, nextVecs models.Vectors,
    id strfmt.UUID, 
    class *models.Class,
) (*models.Object, error) {
    // ... merge properties ...
    
    obj := &models.Object{Class: class.Class, Properties: mergedProps, Vector: vector, Vectors: vectors, ID: id}
    
    // Line 234: UpdateVector with findObject callback
    if err := m.modulesProvider.UpdateVector(ctx, obj, class, m.findObject, m.logger); err != nil {
        return nil, err
    }
    
    // Line 239-264: If vectorizer is "none", preserve old vectors
    if obj.Vector == nil && class.Vectorizer == config.VectorizerModuleNone {
        obj.Vector = prevVec
    }
    
    for name, vectorConfig := range class.VectorConfig {
        if _, ok := vectorConfig.Vectorizer.(map[string]interface{})[config.VectorizerModuleNone]; !ok {
            continue  // Not "none" vectorizer, skip
        }
        
        prevTargetVector, ok := prevVecs[name]
        if !ok {
            continue
        }
        
        if _, ok := obj.Vectors[name]; !ok {
            obj.Vectors[name] = prevTargetVector  // Preserve old vector
        }
    }
    
    return obj, nil
}
```

**KEY INSIGHT**: MergeObject also fetches to:
1. Merge previous properties with new properties (line 223-228)
2. Preserve previous vectors when vectorizer is "none" (lines 239-264)

The second point is interesting: if properties are updated but vectorizer is "none", the code needs to preserve the old vector. However, this logic could be deferred to the RAFT FSM if we pass both old and new vectors through the merge document.

---

## AddObject Flow (add.go, lines 71-127)

For comparison - AddObject does NOT fetch the existing object:

```
Line 96:  m.validateObjectAndNormalizeNames(ctx, repl, object, nil, fetchedClasses)
          └─ existing param is nil!

Line 108: m.modulesProvider.UpdateVector(ctx, object, class, m.findObject, m.logger)
          └─ Same UpdateVector call, but:
             └─ m.findObject will be called IF reVectorizeEmbeddings needs to compare
             └─ But this is a NEW object, so the comparison will find nothing and force re-vectorization
             └─ Result: the vectorizer ALWAYS runs for new objects (no comparison optimization)
```

**CRITICAL**: AddObject SUCCESSFULLY generates vectors without fetching any existing object. The `findObject` callback is only used IF the vectorization system needs to compare old vs new properties - but for Add, that comparison always results in "force vectorization" because the object doesn't exist yet.

---

## What Actually Needs the Existing Object

### For UpdateObject:

1. **Creation time preservation** (line 108) - MUST have
   - Solution: Store in metadata or pass through RAFT FSM

2. **Vectorization comparison** (inside UpdateVector)
   - Currently: Compares old vs new properties to decide if re-vectorization is needed
   - Problem: Requires fetching old object with specific properties
   - Solution: Could defer comparison to FSM, but requires passing old object state through RAFT

3. **Validation - cRef checks** (validateObjectAndNormalizeNames)
   - Current behavior: Validates that cross-reference targets exist
   - Existing object param use: ONLY for tenant mismatch check (line 50 of properties_validation.go)
   - Impact: Actually very minimal - just ensures tenant consistency

### For MergeObject:

1. **Property merging** (lines 223-228)
   - MUST merge old properties with new properties
   - Solution: Could defer to FSM if we pass old properties through

2. **Vector preservation for "none" vectorizer** (lines 239-264)
   - Preserves old vectors if vectorizer is "none"
   - Solution: Could defer to FSM if we pass old vectors through

3. **Same as Update**: Creation time (implicitly - merge doesn't set it, so old one is used)

4. **Same as Update**: Vectorization comparison (for "normal" vectorizers, not "none")

---

## Critical Blocking Issues for Deferral to FSM

### Issue 1: The findObject Callback is Nested Deep

The `findObject` callback passed to `UpdateVector()` is invoked **conditionally** deep inside `reVectorizeEmbeddings()`, which:
- Is a comparison function
- Makes a decision based on whether old and new properties match
- Returns a flag: `shouldReVectorize: bool`

To move this to the FSM, you would need to:
1. Pre-compute `shouldReVectorize` in the handler before RAFT
2. Pass the flag + old object state through RAFT
3. Reconstruct the logic in the FSM

**Problem**: The handler currently makes this decision AFTER vectorization. Moving it before would require refactoring the vectorization flow.

### Issue 2: AddObject Shows It's Possible, But Different

AddObject shows that vectorization CAN work without fetching the existing object because:
- For a NEW object, `findObjectFn` will error or return nil
- This causes `reVectorizeEmbeddings()` to return `shouldReVectorize=true`
- The vectorizer then runs unconditionally

For UPDATE/MERGE, you'd want the optimization (skip vectorization if properties unchanged), which requires the comparison. But the comparison needs the old object state.

### Issue 3: Multi-Vector and Named Vector Support

The system supports:
- Single vector (object.Vector)
- Multiple named vectors (object.Vectors[name])
- ReferenceVectorizer (needs findObjectFn for ref2Vec lookups)

Each has slightly different handling in `UpdateVector()`. Moving this to FSM would require significant refactoring.

---

## Operations That ARE Deferrable

Based on this analysis, here's what COULD theoretically be deferred:

1. **Creation time preservation** (Update only)
   - MINIMAL COST: Just a single int64 field
   - Could be handled by: Pass `was_update: true` flag in RAFT, FSM reads old object time
   - Or: Pass `original_creation_time` in the command

2. **Property merging** (Merge only)
   - MODERATE COST: Merge old + new properties
   - Could be handled by: Pass old object's properties in merge document
   - Risk: Replication consistency if old object state is stale

3. **Vector preservation for "none" vectorizer** (Merge only)
   - MINIMAL COST: Just a vector copy
   - Could be handled by: FSM reads old vectors if vectorizer is "none"
   - Or: Pass old vectors in merge document

---

## Vectorization Comparison: The Core Blocker

The **biggest blocker** for deferring the fetch is the vectorization comparison:

**Current approach:**
```
Handler:
  fetch existing object
  merge properties with updates
  call UpdateVector()
    which calls findObjectFn() (via reVectorizeEmbeddings)
      which fetches old object AGAIN
  vectorizer runs if properties changed
  
FSM:
  writes merged object with new vectors
```

**To defer to FSM, you'd need:**
```
Handler:
  (cannot fetch old object during update/merge)
  merge properties with... what? unknown old properties
  call UpdateVector()
    which can't do comparison without old properties
  vectorizer runs unconditionally
  
FSM:
  gets merged object with vectors
  writes it
  (no comparison optimization)
```

The cost of unconditional vectorization for every update/merge is potentially significant if:
- Vectorizer is expensive (e.g., remote API calls)
- Most updates don't change vectorizable properties
- The comparison optimization saves those calls

---

## Answer to Original Questions

### Q: What fields from existing object are used downstream?
A:
- **Update**: `Created` timestamp (1 field)
- **Update**: Property values (for vectorization comparison via callback)
- **Merge**: ALL properties (for merging) + all vectors (for preservation if vectorizer="none")
- Both: None from validation (existing param only used for tenant check)

### Q: Could these be deferred to FSM?
A:
- **Partially yes** for metadata (creation time, vector preservation)
- **No** for vectorization comparison optimization - would require passing old object state through RAFT
- **No** for property merging in Merge - would require passing old properties through

### Q: Is vectorization the main blocker?
A:
- **YES**: The optimization to skip vectorization when properties don't change requires comparing old vs new properties
- This comparison is deeply nested in the reVectorizeEmbeddings() logic
- Moving it would require significant refactoring of the vectorization pipeline

### Q: Does AddObject vectorize without existing object?
A:
- **YES**: AddObject does NOT fetch existing object before vectorization
- The `findObjectFn` callback is called but immediately returns an error (object doesn't exist)
- This triggers unconditional vectorization (no comparison optimization)
- This shows the pattern works but without the comparison optimization

---

## Architecture Impact Summary

### Current State:
- Handler fetches existing object (1 network call)
- Passes object to vectorization layer
- Vectorization layer may fetch object AGAIN via findObjectFn (for comparison)
- All pre-RAFT validation and processing uses existing object
- RAFT FSM receives: merged object + vectors (no reference to original state)

### Why It's This Way:
1. Comparison optimization requires knowing old property values
2. Creation time must be preserved (immutable field)
3. Property merging needs source properties
4. Vector preservation for "none" vectorizer needs old vectors

### If You Deferred Everything:
- Pros: Handler doesn't know about storage layer
- Cons: Lose vectorization comparison optimization (every update/merge revectorizes)
- Cons: FSM becomes stateful (needs to know about object schema, vectorizers, etc.)
- Cons: Complex to maintain consistency across replicas
