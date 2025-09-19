//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package resolver_test

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/big"
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/resolver"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// fakeSchemaReader is a single test fake that satisfies resolver.schemaReader
// (and multitenancy.schemaReader). It can behave as:
//   - A fixed-shard mapper: set shards = []string{"shard1"} to always return "shard1" for all UUIDs.
//   - A hashing mapper: set shards = []string{...} to distribute UUIDs across shard using a simple hash on the first byte of the UUID.
//   - A tenant mapper: set tenantShards = map[string]string{"tenantA": models.TenantActivityStatusHOT} to
//     distribute UUIDs across tenants based on the tenant name.
//
// Example configs:
//
//	&fakeSchemaReader{shards: []string{"shard1"}} // UUID-hashing distribution with fixed shard (always "shard1")
//	&fakeSchemaReader{shards: []string{"shard1","shard2","shard3"}} // UUID-hashing distribution
//	&fakeSchemaReader{tenantShards: map[string]string{"tenantA": models.TenantActivityStatusHOT}} // tenant sharding distribution
type fakeSchemaReader struct {
	shards          []string
	tenantShards    map[string]string
	tenantsShardErr error
}

// ShardFromUUID assigns a shard by hashing the first byte of the UUID and
// modding it by the number of configured shards. If exactly one shard is
// configured, all UUIDs resolve to that shard (X mod 1 is always 0, resolving
// always to the first and only available shard).
//
// If no shards are configured, the empty string is returned.
func (f *fakeSchemaReader) ShardFromUUID(_ string, uuidBytes []byte) string {
	if len(f.shards) == 0 || len(uuidBytes) == 0 {
		return ""
	}
	return f.shards[int(uuidBytes[0])%len(f.shards)]
}

// TenantsShards returns a copy of tenantShards or the configured error.
func (f *fakeSchemaReader) TenantsShards(_ context.Context, _ string, _ ...string) (map[string]string, error) {
	if f.tenantsShardErr != nil {
		return nil, f.tenantsShardErr
	}
	out := make(map[string]string, len(f.tenantShards))
	for tenant, status := range f.tenantShards {
		out[tenant] = status
	}
	return out, nil
}

// ReadOnlyClass returns a minimal class stub for the given name.
func (f *fakeSchemaReader) ReadOnlyClass(class string) *models.Class {
	return &models.Class{Class: class}
}

// newTestObject builds a storobj.Object with the provided ID and tenant.
func newTestObject(id strfmt.UUID, tenant string) *storobj.Object {
	return &storobj.Object{Object: models.Object{ID: id, Tenant: tenant}}
}

// randIntBetween returns a random integer n in [min, max] (inclusive).
func randIntBetween(t *testing.T, min, max int) int {
	t.Helper()
	require.LessOrEqual(t, min, max)
	n := int64(max - min + 1)
	r, err := crand.Int(crand.Reader, big.NewInt(n))
	require.NoError(t, err)
	return int(r.Int64()) + min
}

func Test_ShardTargets_GroupByShard(t *testing.T) {
	// GIVEN
	object1 := newTestObject(strfmt.UUID(uuid.NewString()), "")
	object2 := newTestObject(strfmt.UUID(uuid.NewString()), "")
	object3 := newTestObject(strfmt.UUID(uuid.NewString()), "")
	targets := resolver.ShardTargets{
		{Shard: "shard-a", Object: object1},
		{Shard: "shard-b", Object: object2},
		{Shard: "shard-a", Object: object3},
	}

	// WHEN
	grouped := targets.GroupByShard()

	// THEN
	require.Len(t, grouped, 2)
	require.Equal(t, []*storobj.Object{object1, object3}, grouped["shard-a"])
	require.Equal(t, []*storobj.Object{object2}, grouped["shard-b"])
}

func Test_ShardTargets_ShardNames_and_Len(t *testing.T) {
	// GIVEN
	targets := resolver.ShardTargets{{Shard: "shard-a"}, {Shard: "shard-b"}, {Shard: "shard-a"}}

	// WHEN
	names := targets.Shards()
	sort.Strings(names)

	// THEN
	require.Equal(t, 3, targets.Len())
	require.Equal(t, []string{"shard-a", "shard-b"}, names)
}

func Test_ShardTargets_EmptyTargets(t *testing.T) {
	// GIVEN
	var targets resolver.ShardTargets

	// WHEN
	grouped := targets.GroupByShard()
	shards := targets.Shards()
	length := targets.Len()

	// THEN
	require.Empty(t, grouped)
	require.Empty(t, shards)
	require.Equal(t, 0, length)
}

func Test_ShardResolution_SingleTenant(t *testing.T) {
	testCases := []struct {
		name           string
		shards         []string
		objectIDs      []strfmt.UUID
		expectError    bool
		expectedShards []string
		errorContains  string
	}{
		{
			name:           "single shard single object",
			shards:         []string{"shard1"},
			objectIDs:      []strfmt.UUID{strfmt.UUID(uuid.NewString())},
			expectedShards: []string{"shard1"},
		},
		{
			name:           "multiple shards single object first byte 0x00",
			shards:         []string{"shard1", "shard2"},
			objectIDs:      []strfmt.UUID{"00aaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
			expectedShards: []string{"shard1"},
		},
		{
			name:           "multiple shards single object first byte 0x01",
			shards:         []string{"shard1", "shard2"},
			objectIDs:      []strfmt.UUID{"01bbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"},
			expectedShards: []string{"shard2"},
		},
		{
			name:          "empty object id",
			shards:        []string{"shard1"},
			objectIDs:     []strfmt.UUID{""},
			expectError:   true,
			errorContains: "parse uuid",
		},
		{
			name:        "invalid uuid",
			shards:      []string{"shard1"},
			objectIDs:   []strfmt.UUID{"do-or-do-not-there-is-no-try"},
			expectError: true,
		},
		{
			name:           "no shards",
			shards:         []string{},
			objectIDs:      []strfmt.UUID{strfmt.UUID(uuid.NewString())},
			expectedShards: []string{""},
		},
		{
			name:           "batch processing single shard",
			shards:         []string{"shard-1"},
			objectIDs:      []strfmt.UUID{strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString())},
			expectedShards: []string{"shard-1", "shard-1"},
		},
		{
			name:           "batch processing multiple shards",
			shards:         []string{"shard1", "shard2"},
			objectIDs:      []strfmt.UUID{"00aaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "01bbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"},
			expectedShards: []string{"shard1", "shard2"},
		},
		{
			name:          "batch with empty object id",
			shards:        []string{"shard1"},
			objectIDs:     []strfmt.UUID{strfmt.UUID(uuid.NewString()), ""},
			expectError:   true,
			errorContains: "parse uuid",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN
			schemaReader := &fakeSchemaReader{shards: tc.shards}
			r := resolver.NewBuilder("TestClass", false, schemaReader).Build()

			objects := make([]*storobj.Object, len(tc.objectIDs))
			for i, id := range tc.objectIDs {
				objects[i] = newTestObject(id, "")
			}

			if len(objects) == 1 {
				// WHEN (single object)
				target, err := r.ResolveShard(context.Background(), objects[0])

				// THEN
				if tc.expectError {
					require.Error(t, err)
					if tc.errorContains != "" {
						require.Contains(t, err.Error(), tc.errorContains)
					}
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.expectedShards[0], target.Shard)
					require.Same(t, objects[0], target.Object)
				}
			} else {
				// WHEN (batch)
				batch, err := r.ResolveShards(context.Background(), objects)

				// THEN
				if tc.expectError {
					require.Error(t, err)
					if tc.errorContains != "" {
						require.Contains(t, err.Error(), tc.errorContains)
					}
				} else {
					require.NoError(t, err)
					require.Len(t, batch, len(objects))

					actualShards := make([]string, len(batch))
					for i, target := range batch {
						actualShards[i] = target.Shard
					}
					require.ElementsMatch(t, tc.expectedShards, actualShards)

					actualObjects := make([]*storobj.Object, len(batch))
					for i, target := range batch {
						actualObjects[i] = target.Object
					}
					require.ElementsMatch(t, objects, actualObjects)
				}
			}
		})
	}
}

func Test_ShardResolution_MultiTenant(t *testing.T) {
	testCases := []struct {
		name           string
		tenantShards   map[string]string
		objectIDs      []strfmt.UUID
		tenants        []string
		expectError    bool
		expectedShards []string
	}{
		{
			name: "valid tenant single object",
			tenantShards: map[string]string{
				"tenantA": models.TenantActivityStatusHOT,
			},
			objectIDs:      []strfmt.UUID{strfmt.UUID(uuid.NewString())},
			tenants:        []string{"tenantA"},
			expectedShards: []string{"tenantA"},
		},
		{
			name: "invalid uuid doesn't matter in tenant mode",
			tenantShards: map[string]string{
				"tenantA": models.TenantActivityStatusHOT,
			},
			objectIDs:      []strfmt.UUID{"do-or-do-not-there-is-no-try"},
			tenants:        []string{"tenantA"},
			expectedShards: []string{"tenantA"},
		},
		{
			name: "batch processing multiple tenants",
			tenantShards: map[string]string{
				"tenantA": models.TenantActivityStatusHOT,
				"tenantB": models.TenantActivityStatusHOT,
			},
			objectIDs:      []strfmt.UUID{strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString())},
			tenants:        []string{"tenantA", "tenantA", "tenantB"},
			expectedShards: []string{"tenantA", "tenantA", "tenantB"},
		},
		{
			name: "empty tenant in batch",
			tenantShards: map[string]string{
				"tenantA": models.TenantActivityStatusHOT,
			},
			objectIDs:   []strfmt.UUID{strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString())},
			tenants:     []string{"tenantA", ""},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN
			schemaReader := &fakeSchemaReader{tenantShards: tc.tenantShards}
			r := resolver.NewBuilder("TestClass", true, schemaReader).Build()

			objects := make([]*storobj.Object, len(tc.objectIDs))
			for i, id := range tc.objectIDs {
				objects[i] = newTestObject(id, tc.tenants[i])
			}
			if len(objects) == 1 {
				// WHEN (single object)
				target, err := r.ResolveShard(context.Background(), objects[0])

				// THEN
				if tc.expectError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.expectedShards[0], target.Shard)
					require.Same(t, objects[0], target.Object)
				}
			} else {
				// WHEN (batch)
				batch, err := r.ResolveShards(context.Background(), objects)

				// THEN
				if tc.expectError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Len(t, batch, len(objects))

					actualShards := make([]string, len(batch))
					for i, target := range batch {
						actualShards[i] = target.Shard
					}
					require.ElementsMatch(t, tc.expectedShards, actualShards)

					actualObjects := make([]*storobj.Object, len(batch))
					for i, target := range batch {
						actualObjects[i] = target.Object
					}
					require.ElementsMatch(t, objects, actualObjects)

					if len(objects) > 1 && !tc.expectError {
						groups := batch.GroupByShard()
						shardNames := batch.Shards()

						expectedGroups := make(map[string]int)
						for _, shard := range tc.expectedShards {
							expectedGroups[shard]++
						}

						for shardName, expectedCount := range expectedGroups {
							require.Len(t, groups[shardName], expectedCount, "shard %q should have %d objects", shardName, expectedCount)
						}

						expectedUniqueShards := make(map[string]struct{})
						for _, shard := range tc.expectedShards {
							expectedUniqueShards[shard] = struct{}{}
						}
						actualUniqueShards := make([]string, 0, len(expectedUniqueShards))
						for shard := range expectedUniqueShards {
							actualUniqueShards = append(actualUniqueShards, shard)
						}
						require.ElementsMatch(t, actualUniqueShards, shardNames)
					}
				}
			}
		})
	}
}

func Test_ShardResolution_SchemaReaderError(t *testing.T) {
	// GIVEN
	schemaReader := &fakeSchemaReader{
		tenantsShardErr: fmt.Errorf("schema reader error"),
	}
	r := resolver.NewBuilder("TestClass", true, schemaReader).Build()
	objects := []*storobj.Object{
		newTestObject(strfmt.UUID(uuid.NewString()), "tenantA"),
	}

	// WHEN
	targets, err := r.ResolveShards(context.Background(), objects)

	// THEN
	require.Error(t, err)
	require.Empty(t, targets)
	require.Contains(t, err.Error(), "fetch tenant status")
	require.Contains(t, err.Error(), "schema reader error")
}

func Test_ShardResolution_TenantValidationError(t *testing.T) {
	// GIVEN - Tenant exists but is not active
	schemaReader := &fakeSchemaReader{
		tenantShards: map[string]string{
			"tenantA": models.TenantActivityStatusCOLD,
		},
	}
	r := resolver.NewBuilder("TestClass", true, schemaReader).Build()
	objects := []*storobj.Object{
		newTestObject(strfmt.UUID(uuid.NewString()), "tenantA"),
	}

	// WHEN
	targets, err := r.ResolveShards(context.Background(), objects)

	// THEN
	require.Error(t, err)
	require.Empty(t, targets)
	require.Contains(t, err.Error(), "tenant not active")
}

func Test_ShardResolution_EmptyInputs(t *testing.T) {
	testCases := []struct {
		name                string
		multiTenancyEnabled bool
		tenant              string
		tenantShards        map[string]string
		shards              []string
	}{
		{
			name:                "single tenant",
			multiTenancyEnabled: false,
			shards:              []string{"shard1"},
		},
		{
			name:                "multi tenant",
			multiTenancyEnabled: true,
			tenantShards:        map[string]string{"tenantA": models.TenantActivityStatusHOT},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN
			schemaReader := &fakeSchemaReader{
				shards:       tc.shards,
				tenantShards: tc.tenantShards,
			}
			r := resolver.NewBuilder("TestClass", tc.multiTenancyEnabled, schemaReader).Build()

			// WHEN
			targets, err := r.ResolveShards(context.Background(), []*storobj.Object{})

			// THEN
			require.NoError(t, err)
			require.Empty(t, targets)
			require.Equal(t, 0, targets.Len())
		})
	}
}

func TestResolver_SingleTenant_RandomObjects_RandomShards(t *testing.T) {
	// GIVEN
	numShards := randIntBetween(t, 3, 5)
	numObjects := randIntBetween(t, 10, 20)

	shards := make([]string, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = fmt.Sprintf("shard-%d", i)
	}
	schemaReader := &fakeSchemaReader{shards: shards}
	r := resolver.NewBuilder("RandClassST", false, schemaReader).Build()

	objects := make([]*storobj.Object, 0, numObjects)
	for i := 0; i < numObjects; i++ {
		objects = append(objects, newTestObject(strfmt.UUID(uuid.NewString()), ""))
	}

	// WHEN
	targets, err := r.ResolveShards(context.Background(), objects)
	require.NoError(t, err)
	grouped := targets.GroupByShard()
	names := targets.Shards()

	// THEN
	require.Len(t, targets, numObjects)

	allowed := make(map[string]struct{}, len(shards))
	for _, shard := range shards {
		allowed[shard] = struct{}{}
	}
	for i := range objects {
		require.Same(t, objects[i], targets[i].Object)
		_, ok := allowed[targets[i].Shard]
		require.True(t, ok, "resolved shard %q is not in allowed list", targets[i].Shard)
	}

	total := 0
	groupKeys := make([]string, 0, len(grouped))
	for key, objs := range grouped {
		groupKeys = append(groupKeys, key)
		total += len(objs)
	}
	require.Equal(t, numObjects, total)
	require.ElementsMatch(t, groupKeys, names)
	require.LessOrEqual(t, len(names), numShards)
}

func Test_ShardResolution_SingleTenant_WithTenant(t *testing.T) {
	// GIVEN
	schemaReader := &fakeSchemaReader{shards: []string{"shard1"}}
	r := resolver.NewBuilder("TestClass", false, schemaReader).Build()
	object := newTestObject(strfmt.UUID(uuid.NewString()), "sometenant")

	// WHEN
	_, err := r.ResolveShard(context.Background(), object)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "multi-tenancy disabled")
}

func TestResolver_MultiTenant_RandomObjects_RandomShards(t *testing.T) {
	// GIVEN
	numTenants := randIntBetween(t, 2, 5)
	numObjects := randIntBetween(t, 10, 20)

	tenants := make([]string, numTenants)
	tenantStatus := make(map[string]string, numTenants)
	for i := 0; i < numTenants; i++ {
		tenants[i] = fmt.Sprintf("tenant-%c", i)
		tenantStatus[tenants[i]] = models.TenantActivityStatusHOT
	}
	schemaReader := &fakeSchemaReader{tenantShards: tenantStatus}
	res := resolver.NewBuilder("RandClassMT", true, schemaReader).Build()

	objects := make([]*storobj.Object, 0, numObjects)
	for i := 0; i < numObjects; i++ {
		tenantIdx := randIntBetween(t, 0, len(tenants)-1)
		objects = append(objects, newTestObject(strfmt.UUID(uuid.NewString()), tenants[tenantIdx]))
	}

	// WHEN
	targets, err := res.ResolveShards(context.Background(), objects)
	require.NoError(t, err)
	grouped := targets.GroupByShard()
	names := targets.Shards()

	// THEN
	require.Len(t, targets, numObjects)
	for i := range objects {
		require.Same(t, objects[i], targets[i].Object)
		require.Equal(t, objects[i].Object.Tenant, targets[i].Shard)
	}

	total := 0
	groupKeys := make([]string, 0, len(grouped))
	allowed := make(map[string]struct{}, len(tenants))
	for _, tenant := range tenants {
		allowed[tenant] = struct{}{}
	}
	for key, objs := range grouped {
		groupKeys = append(groupKeys, key)
		total += len(objs)
		_, ok := allowed[key]
		require.True(t, ok, "group key %q not in tenants set", key)
	}
	require.Equal(t, numObjects, total)
	require.ElementsMatch(t, groupKeys, names)
	require.LessOrEqual(t, len(names), len(tenants))
}

func Test_ShardResolution_MultiTenant_MixedValidInvalid(t *testing.T) {
	// GIVEN
	tenantShards := map[string]string{
		"validTenant": models.TenantActivityStatusHOT,
	}
	schemaReader := &fakeSchemaReader{tenantShards: tenantShards}
	r := resolver.NewBuilder("TestClass", true, schemaReader).Build()

	objects := []*storobj.Object{
		newTestObject(strfmt.UUID(uuid.NewString()), "validTenant"),
		newTestObject(strfmt.UUID(uuid.NewString()), "invalidTenant"),
	}

	// WHEN
	_, err := r.ResolveShards(context.Background(), objects)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not found")
}

func Test_ShardResolution_MultiTenant_DuplicateTenants(t *testing.T) {
	// GIVEN
	tenantShards := map[string]string{
		"tenantA": models.TenantActivityStatusHOT,
	}
	schemaReader := &fakeSchemaReader{tenantShards: tenantShards}
	r := resolver.NewBuilder("TestClass", true, schemaReader).Build()

	objects := []*storobj.Object{
		newTestObject(strfmt.UUID(uuid.NewString()), "tenantA"),
		newTestObject(strfmt.UUID(uuid.NewString()), "tenantA"),
		newTestObject(strfmt.UUID(uuid.NewString()), "tenantA"),
	}

	// WHEN
	targets, err := r.ResolveShards(context.Background(), objects)

	// THEN
	require.NoError(t, err)
	require.Len(t, targets, 3)

	grouped := targets.GroupByShard()
	require.Len(t, grouped, 1)
	require.Len(t, grouped["tenantA"], 3)
}
