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

package schema

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestMakeTenantWithBelongsToNodes(t *testing.T) {
	const tenant = "tenant"
	const status = "status"

	physical := sharding.Physical{
		Status:         status,
		BelongsToNodes: []string{"node1"},
	}

	t.Run("Creates valid response", func(t *testing.T) {
		tenantResp := makeTenantResponse(tenant, physical)

		assert.Equal(t, tenant, tenantResp.Name)
		assert.Equal(t, status, tenantResp.ActivityStatus)
		assert.ElementsMatch(t, tenantResp.BelongsToNodes, physical.BelongsToNodes)
	})

	t.Run("BelongsToNodes is a copy", func(t *testing.T) {
		tenantResp := makeTenantResponse(tenant, physical)

		assert.NotEqual(t,
			reflect.ValueOf(tenantResp.BelongsToNodes).Pointer(),
			reflect.ValueOf(physical.BelongsToNodes).Pointer())
	})
}
