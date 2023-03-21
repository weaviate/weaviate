//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/usecases/replica"
)

func getClass(t *testing.T, host, class string) *models.Class {
	helper.SetupClient(host)
	return helper.GetClass(t, class)
}

func updateClass(t *testing.T, host string, class *models.Class) {
	helper.SetupClient(host)
	helper.UpdateClass(t, class)
}

func createObject(t *testing.T, host string, obj *models.Object) {
	helper.SetupClient(host)
	helper.CreateObject(t, obj)
}

func createObjectCL(t *testing.T, host string, obj *models.Object, cl replica.ConsistencyLevel) {
	helper.SetupClient(host)
	helper.CreateObjectCL(t, obj, cl)
}

func createObjects(t *testing.T, host string, batch []*models.Object) {
	helper.SetupClient(host)
	helper.CreateObjectsBatch(t, batch)
}

func getObject(t *testing.T, host, class string, id strfmt.UUID) (*models.Object, error) {
	helper.SetupClient(host)
	return helper.GetObject(t, class, id)
}

func objectExistsCL(t *testing.T, host, class string, id strfmt.UUID, cl replica.ConsistencyLevel) (bool, error) {
	helper.SetupClient(host)
	return helper.ObjectExistsCL(t, class, id, cl)
}

func getObjectCL(t *testing.T, host, class string, id strfmt.UUID, cl replica.ConsistencyLevel) (*models.Object, error) {
	helper.SetupClient(host)
	return helper.GetObjectCL(t, class, id, cl)
}

func getObjectFromNode(t *testing.T, host, class string, id strfmt.UUID, nodename string) (*models.Object, error) {
	helper.SetupClient(host)
	return helper.GetObjectFromNode(t, class, id, nodename)
}

func patchObject(t *testing.T, host string, patch *models.Object) {
	helper.SetupClient(host)
	helper.PatchObject(t, patch)
}

func updateObjectCL(t *testing.T, host string, obj *models.Object, cl replica.ConsistencyLevel) {
	helper.SetupClient(host)
	helper.UpdateObjectCL(t, obj, cl)
}

func addReferences(t *testing.T, host string, refs []*models.BatchReference) {
	helper.SetupClient(host)
	helper.AddReferences(t, refs)
}

func deleteObject(t *testing.T, host, class string, id strfmt.UUID) {
	helper.SetupClient(host)

	toDelete, err := helper.GetObject(t, class, id)
	require.Nil(t, err)

	helper.DeleteObject(t, toDelete)

	_, err = helper.GetObject(t, class, id)
	assert.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
}

func deleteObjects(t *testing.T, host, class string, path []string, valueString string) {
	helper.SetupClient(host)

	batchDelete := &models.BatchDelete{
		Match: &models.BatchDeleteMatch{
			Class: class,
			Where: &models.WhereFilter{
				Operator:    filters.OperatorLike.Name(),
				Path:        path,
				ValueString: &valueString,
			},
		},
	}
	helper.DeleteObjectsBatch(t, batchDelete)

	resp := gqlGet(t, host, class, replica.All)
	assert.Empty(t, resp)
}

func gqlGet(t *testing.T, host, class string, cl replica.ConsistencyLevel, fields ...string) []interface{} {
	helper.SetupClient(host)

	if cl == "" {
		cl = replica.Quorum
	}

	q := fmt.Sprintf("{Get {%s (consistencyLevel: %s)", class, cl) + " {%s}}}"
	if len(fields) == 0 {
		fields = []string{"_additional{id}"}
	}
	q = fmt.Sprintf(q, strings.Join(fields, " "))

	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, q)

	result := resp.Get("Get").Get(class)
	return result.Result.([]interface{})
}

func getNodes(t *testing.T, host string) *models.NodesStatusResponse {
	helper.SetupClient(host)

	resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
	helper.AssertRequestOk(t, resp, err, nil)
	return resp.Payload
}
