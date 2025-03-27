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

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// stopNodeAt stops the node container at the given index.
//
// NOTE: the index is 1-based, so stopping the first node requires index=1, not 0
func StopNodeAt(ctx context.Context, t *testing.T, compose *docker.DockerCompose, index int) {
	<-time.After(1 * time.Second)
	if err := compose.StopAt(ctx, index, nil); err != nil {
		// try one more time after 1 second
		<-time.After(1 * time.Second)
		require.NoError(t, compose.StopAt(ctx, index, nil))
	}
	<-time.After(1 * time.Second) // give time for shutdown
}

// startNodeAt starts the node container at the given index.
//
// NOTE: the index is 1-based, so starting the first node requires index=1, not 0
func StartNodeAt(ctx context.Context, t *testing.T, compose *docker.DockerCompose, index int) {
	t.Helper()
	if err := compose.StartAt(ctx, index); err != nil {
		// try one more time after 1 second
		<-time.After(1 * time.Second)
		require.NoError(t, compose.StartAt(ctx, index))
	}
	<-time.After(1 * time.Second)
}

func GetClass(t *testing.T, host, class string) *models.Class {
	t.Helper()
	helper.SetupClient(host)
	return helper.GetClass(t, class)
}

func UpdateClass(t *testing.T, host string, class *models.Class) {
	t.Helper()
	helper.SetupClient(host)
	helper.UpdateClass(t, class)
}

func CreateObjectCL(t *testing.T, host string, obj *models.Object, cl types.ConsistencyLevel) error {
	t.Helper()
	helper.SetupClient(host)
	return helper.CreateObjectCL(t, obj, cl)
}

func CreateObjects(t *testing.T, host string, batch []*models.Object) {
	t.Helper()
	helper.SetupClient(host)
	helper.CreateObjectsBatch(t, batch)
}

func CreateObjectsCL(t *testing.T, host string, batch []*models.Object, cl types.ConsistencyLevel) {
	helper.SetupClient(host)
	helper.CreateObjectsBatchCL(t, batch, cl)
}

func CreateTenantObjects(t *testing.T, host string, batch []*models.Object) {
	t.Helper()
	helper.SetupClient(host)
	helper.CreateObjectsBatch(t, batch)
}

func GetObject(t *testing.T, host, class string, id strfmt.UUID, withVec bool) (*models.Object, error) {
	t.Helper()
	helper.SetupClient(host)
	var include string
	if withVec {
		include = "vector"
	}
	return helper.GetObject(t, class, id, include)
}

func GetTenantObject(t *testing.T, host, class string, id strfmt.UUID, tenant string) (*models.Object, error) {
	t.Helper()
	helper.SetupClient(host)
	return helper.TenantObject(t, class, id, tenant)
}

func ObjectExistsCL(t *testing.T, host, class string, id strfmt.UUID, cl types.ConsistencyLevel) (bool, error) {
	t.Helper()
	helper.SetupClient(host)
	return helper.ObjectExistsCL(t, class, id, cl)
}

func GetObjectCL(t *testing.T, host, class string, id strfmt.UUID, cl types.ConsistencyLevel) (*models.Object, error) {
	t.Helper()
	helper.SetupClient(host)
	return helper.GetObjectCL(t, class, id, cl)
}

func GetObjectFromNode(t *testing.T, host, class string, id strfmt.UUID, nodename string) (*models.Object, error) {
	t.Helper()
	helper.SetupClient(host)
	return helper.GetObjectFromNode(t, class, id, nodename)
}

func GetTenantObjectFromNode(t *testing.T, host, class string, id strfmt.UUID, nodename, tenant string) (*models.Object, error) {
	t.Helper()
	helper.SetupClient(host)
	return helper.GetTenantObjectFromNode(t, class, id, nodename, tenant)
}

func PatchObject(t *testing.T, host string, patch *models.Object) {
	t.Helper()
	helper.SetupClient(host)
	helper.PatchObject(t, patch)
}

func UpdateObjectCL(t *testing.T, host string, obj *models.Object, cl types.ConsistencyLevel) error {
	t.Helper()
	helper.SetupClient(host)
	return helper.UpdateObjectCL(t, obj, cl)
}

func AddReferences(t *testing.T, host string, refs []*models.BatchReference) {
	t.Helper()
	helper.SetupClient(host)
	resp, err := helper.AddReferences(t, refs)
	helper.CheckReferencesBatchResponse(t, resp, err)
}

func AddTenantReferences(t *testing.T, host string, refs []*models.BatchReference) {
	t.Helper()
	helper.SetupClient(host)
	resp, err := helper.AddReferences(t, refs)
	helper.CheckReferencesBatchResponse(t, resp, err)
}

func DeleteObject(t *testing.T, host, class string, id strfmt.UUID, cl types.ConsistencyLevel) {
	t.Helper()
	helper.SetupClient(host)

	_, err := helper.GetObject(t, class, id)
	require.Nil(t, err)

	helper.DeleteObjectCL(t, class, id, cl)

	_, err = helper.GetObject(t, class, id)
	require.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
}

func DeleteTenantObject(t *testing.T, host, class string, id strfmt.UUID, tenant string, cl types.ConsistencyLevel) {
	helper.SetupClient(host)
	helper.DeleteTenantObject(t, class, id, tenant, cl)

	_, err := helper.TenantObject(t, class, id, tenant)
	assert.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
}

func DeleteObjects(t *testing.T, host, class string, path []string, valueText string, cl types.ConsistencyLevel) {
	t.Helper()
	helper.SetupClient(host)

	batchDelete := &models.BatchDelete{
		Match: &models.BatchDeleteMatch{
			Class: class,
			Where: &models.WhereFilter{
				Operator:  filters.OperatorLike.Name(),
				Path:      path,
				ValueText: &valueText,
			},
		},
	}
	helper.DeleteObjectsBatch(t, batchDelete, cl)

	resp := GQLGet(t, host, class, types.ConsistencyLevelAll)
	assert.Empty(t, resp)
}

func DeleteTenantObjects(t *testing.T, host, class string, path []string, valueText, tenant string, cl types.ConsistencyLevel) {
	t.Helper()
	helper.SetupClient(host)

	batchDelete := &models.BatchDelete{
		Match: &models.BatchDeleteMatch{
			Class: class,
			Where: &models.WhereFilter{
				Operator:  filters.OperatorLike.Name(),
				Path:      path,
				ValueText: &valueText,
			},
		},
	}
	resp, err := helper.DeleteTenantObjectsBatchCL(t, batchDelete, tenant, cl)
	helper.AssertRequestOk(t, resp, err, nil)

	deleted := GQLTenantGet(t, host, class, types.ConsistencyLevelAll, tenant)
	assert.Empty(t, deleted)
}

func GQLGet(t *testing.T, host, class string, cl types.ConsistencyLevel, fields ...string) []interface{} {
	t.Helper()
	helper.SetupClient(host)

	if cl == "" {
		cl = types.ConsistencyLevelQuorum
	}

	q := fmt.Sprintf("{Get {%s (consistencyLevel: %s)", class, cl) + " {%s}}}"
	if len(fields) == 0 {
		fields = []string{"_additional{id isConsistent vector}"}
	}
	q = fmt.Sprintf(q, strings.Join(fields, " "))

	return GQLDo(t, class, q)
}

func GQLGetNearVec(t *testing.T, host, class string, vec []interface{}, cl types.ConsistencyLevel, fields ...string) []interface{} {
	t.Helper()
	helper.SetupClient(host)

	if cl == "" {
		cl = types.ConsistencyLevelQuorum
	}

	q := fmt.Sprintf("{Get {%s (consistencyLevel: %s, nearVector: {vector: %s, certainty: 0.8})",
		class, cl, Vec2String(vec)) + " {%s}}}"
	if len(fields) == 0 {
		fields = []string{"_additional{id isConsistent}"}
	}
	q = fmt.Sprintf(q, strings.Join(fields, " "))

	return GQLDo(t, class, q)
}

func GQLDo(t *testing.T, class, query string) []interface{} {
	t.Helper()
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)

	result := resp.Get("Get").Get(class)
	return result.Result.([]interface{})
}

func GQLTenantGet(t *testing.T, host, class string, cl types.ConsistencyLevel,
	tenant string, fields ...string,
) []interface{} {
	t.Helper()
	helper.SetupClient(host)

	if cl == "" {
		cl = types.ConsistencyLevelQuorum
	}

	q := fmt.Sprintf("{Get {%s (tenant: %q, consistencyLevel: %s, limit: 1000)", class, tenant, cl) + " {%s}}}"
	if len(fields) == 0 {
		fields = []string{"_additional{id isConsistent}"}
	}
	q = fmt.Sprintf(q, strings.Join(fields, " "))

	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, q)

	result := resp.Get("Get").Get(class)
	return result.Result.([]interface{})
}

func CountObjects(t *testing.T, host, class string) int64 {
	helper.SetupClient(host)

	q := fmt.Sprintf(`{Aggregate{%s{meta{count}}}}`, class)

	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, q)

	result := resp.Get("Aggregate").Get(class).AsSlice()
	require.Len(t, result, 1)
	meta := result[0].(map[string]interface{})["meta"].(map[string]interface{})
	count, err := meta["count"].(json.Number).Int64()
	require.Nil(t, err)
	return count
}

func CountTenantObjects(t *testing.T, host, class string,
	tenant string,
) int64 {
	t.Helper()
	helper.SetupClient(host)

	q := fmt.Sprintf(`{Aggregate{%s(tenant: %q){meta{count}}}}`, class, tenant)

	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, q)

	result := resp.Get("Aggregate").Get(class).AsSlice()
	require.Len(t, result, 1)
	meta := result[0].(map[string]interface{})["meta"].(map[string]interface{})
	count, err := meta["count"].(json.Number).Int64()
	require.Nil(t, err)
	return count
}

func GetNodes(t *testing.T, host string) *models.NodesStatusResponse {
	t.Helper()
	helper.SetupClient(host)
	verbose := verbosity.OutputVerbose
	params := nodes.NewNodesGetParams().WithOutput(&verbose)
	resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
	return resp.Payload
}

func Vec2String(v []interface{}) (s string) {
	for _, n := range v {
		x := n.(json.Number)
		s = fmt.Sprintf("%s, %s", s, x.String())
	}
	s = strings.TrimLeft(s, ", ")
	return fmt.Sprintf("[%s]", s)
}
