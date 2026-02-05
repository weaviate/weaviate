//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package mcp

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	testServerAddr = "localhost:8082"
	testAPIKey     = "admin-key"
)

// createTenantsForClass is a shared helper that creates tenants for a multi-tenant class.
func createTenantsForClass(t *testing.T, className string, tenantNames []string) {
	t.Helper()

	if len(tenantNames) == 0 {
		return
	}

	tenants := make([]*models.Tenant, len(tenantNames))
	for i, name := range tenantNames {
		tenants[i] = &models.Tenant{Name: name}
	}
	helper.CreateTenantsAuth(t, className, tenants, testAPIKey)
}
