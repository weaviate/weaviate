//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package query

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Tenant(t *testing.T) {
	root := os.TempDir()
	defer func() {
		os.RemoveAll(path.Join(root, CachePrefix))
	}()

	versionStr := "0"
	c := NewDiskCache(root, 100) // 100 bytes max cap

	// add tenant1
	collection1 := "test-collection1"
	tenant1 := "test-tenant1"
	key1 := c.TenantKey(collection1, tenant1)
	createTempFileWithSize(t, path.Join(root, CachePrefix, key1, versionStr), t.Name(), 30)
	require.NoError(t, c.AddTenant(collection1, tenant1, 0))

	// make sure the tenant1 is cached
	tt, err := c.Tenant(collection1, tenant1)
	require.NoError(t, err)
	require.NotNil(t, tt)
	assert.NotEmpty(t, tt.AbsolutePath())

	// add tenant2
	collection2 := "test-collection2"
	tenant2 := "test-tenant2"
	key2 := c.TenantKey(collection2, tenant2)
	createTempFileWithSize(t, path.Join(root, CachePrefix, key2, versionStr), t.Name(), 70)
	require.NoError(t, c.AddTenant(collection2, tenant2, 0))

	// make sure the tenant2 is cached
	tt, err = c.Tenant(collection2, tenant2)
	require.NoError(t, err)
	require.NotNil(t, tt)
	assert.NotEmpty(t, tt.AbsolutePath())

	// Now cache reached max capacity of 100 bytes. Anymore addition of tenant should evict
	// add tenant3. This should evict both of the previous tenants, why?
	// with this tenant, usage -> 140 Bytes. But max is 100 Bytes.
	// Removing tenant1 -> frees only 30Bytes, only by freezing both tenants, usage becomes < 100 bytes.
	collection3 := "test-collection3"
	tenant3 := "test-tenant3"
	key3 := c.TenantKey(collection3, tenant3)
	createTempFileWithSize(t, path.Join(root, CachePrefix, key3, versionStr), t.Name(), 70)
	require.NoError(t, c.AddTenant(collection3, tenant3, 0))

	// make sure the tenant3 is cached and other two tenants are evicted
	tt, err = c.Tenant(collection3, tenant3)
	require.NoError(t, err)
	require.NotNil(t, tt)
	assert.NotEmpty(t, tt.AbsolutePath())

	_, err = c.Tenant(collection1, tenant1)
	assert.ErrorIs(t, err, ErrTenantNotFound)

	_, err = c.Tenant(collection2, tenant2)
	assert.ErrorIs(t, err, ErrTenantNotFound)
}

func createTempFileWithSize(t *testing.T, dir, prefix string, size int64) *os.File {
	t.Helper()

	require.NoError(t, os.MkdirAll(dir, 0o777))

	tf, err := os.CreateTemp(dir, prefix)
	if err != nil {
		t.Fatal(err)
	}

	if err := tf.Truncate(size); err != nil {
		tf.Close()
		os.Remove(tf.Name()) // cleanup in case of error
		t.Fatal(err)
	}

	return tf
}
