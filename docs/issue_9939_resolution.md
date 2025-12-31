# Issue 9939 Resolution: Named Vectors Property Configuration

## Issue Description
When using named vectors (default in Python client v4), property configuration such as `skip_vectorization=True` was being ignored. The generated schema showed `skip: false` (or default values) instead of the user-provided configuration.

## Root Cause
The issue stemmed from a mismatch in how property configuration was stored and retrieved when using named vectors.

1.  **Configuration Storage**: The Python client sends property configuration under the vector name (e.g., `default`) when using named vectors.
2.  **Configuration Retrieval**: The server's `SetSinglePropertyDefaults` method was iterating over vectorizers and looking for configuration under the **vectorizer name** (e.g., `text2vec-openai`).
3.  **Result**: The server failed to find the user's configuration under the vectorizer name, so it applied default values (e.g., `skip: false`) and stored them under the vectorizer name.

## Solution
The solution involved updating the module configuration logic to respect named vectors.

### 1. Update `SetSinglePropertyDefaults`
In `usecases/modules/module_config_init_and_validate.go`, the `SetSinglePropertyDefaults` method was updated to pass the `targetVector` name to the internal helper methods.

```go
// Before
p.setSinglePropertyDefaults(prop, vectorizer)

// After
p.setSinglePropertyDefaults(prop, vectorizer, targetVector)
```

### 2. Update `setSinglePropertyConfigDefaults`
The `setSinglePropertyConfigDefaults` method was updated to:
-   Determine the `targetKey` (either the vector name or the vectorizer name).
-   Look for user-specified configuration under the `targetKey`.
-   Store the merged configuration (defaults + user values) under the `targetKey`.

```go
	targetKey := vectorizer
	if targetVector != "" {
		targetKey = targetVector
	}

	if prop.ModuleConfig != nil {
		if vectorizerConfig, ok := prop.ModuleConfig.(map[string]interface{})[targetKey]; ok {
            // ... use user specified config
		}
	}
    // ... merge defaults
    prop.ModuleConfig.(map[string]interface{})[targetKey] = mergedConfig
```

### 3. Update `ClassBasedModuleConfig.Property`
In `usecases/modules/module_config.go`, the `Property` method was updated to look up configuration using the `targetVector` name if available. It also includes a fallback to the module name for backward compatibility.

```go
	targetKey := cbmc.moduleName
	if cbmc.targetVector != "" {
		targetKey = cbmc.targetVector
	}

	moduleCfg, ok := asMap[targetKey]
    // ... fallback logic
```

## Verification
The fix was verified using a reproduction test case (`TestSetSinglePropertyDefaults_NamedVectors_Issue9939`) which simulates the behavior of the Python client v4. The test confirms that:
-   Configuration provided under the vector name is correctly preserved.
-   Default values are correctly merged.
-   Legacy behavior (configuration under vectorizer name) is still supported.

Existing tests in `module_config_init_and_validate_test.go` were also updated to reflect that configuration for named vectors is now stored under the vector name.
