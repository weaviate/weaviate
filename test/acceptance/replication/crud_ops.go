//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createObject(t *testing.T, host string, obj *models.Object) {
	helper.SetupClient(host)
	helper.CreateObject(t, obj)
}

func createObjects(t *testing.T, host string, batch []*models.Object) {
	helper.SetupClient(host)
	helper.CreateObjectsBatch(t, batch)
}

func getObject(t *testing.T, host, class string, id strfmt.UUID) (*models.Object, error) {
	helper.SetupClient(host)
	return helper.GetObject(t, class, id)
}

func patchObject(t *testing.T, host string, patch *models.Object) {
	helper.SetupClient(host)
	helper.PatchObject(t, patch)
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
	assert.Equal(t, err, &objects.ObjectsClassGetNotFound{})
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

	resp := gqlGet(t, host, class)
	assert.Empty(t, resp)
}

func gqlGet(t *testing.T, host, class string, fields ...string) []interface{} {
	helper.SetupClient(host)

	q := "{Get {" + class + " {%s}}}"
	if len(fields) == 0 {
		fields = []string{"_additional{id}"}
	}
	q = fmt.Sprintf(q, strings.Join(fields, " "))

	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, q)

	result := resp.Get("Get").Get(class)
	return result.Result.([]interface{})
}
