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

package test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/exporttest"
)

// TestExport_RejectsWithoutAsyncReplication verifies that export is rejected
// with a 422 when async replication is globally disabled. The cluster is
// started in TestMain with ASYNC_REPLICATION_DISABLED=true; no S3 backend is
// needed because the 422 is returned before any storage operation.
func TestExport_RejectsWithoutAsyncReplication(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	helper.CreateObjectsBatchCL(t, makeObjects(className, 5), types.ConsistencyLevelAll)

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.Error(t, err)
	require.Contains(t, err.Error(), "422",
		"export without async replication should be rejected with 422, got: %v", err)
}

func sanitizeClassName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}

func makeObjects(className string, count int) []*models.Object {
	objects := make([]*models.Object, count)
	for i := range objects {
		objects[i] = &models.Object{
			Class:      className,
			Properties: map[string]interface{}{"text": "object"},
		}
	}
	return objects
}
