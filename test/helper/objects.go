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

package helper

import (
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/client/batch"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

func SetupClient(uri string) {
	host, port := "", ""
	res := strings.Split(uri, ":")
	if len(res) == 2 {
		host, port = res[0], res[1]
	}
	ServerHost = host
	ServerPort = port
}

func CreateClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := Client(t).Schema.SchemaObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := Client(t).Objects.ObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateObjectsBatch(t *testing.T, objects []*models.Object) {
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: objects,
		})
	resp, err := Client(t).Batch.BatchObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
	for _, elem := range resp.Payload {
		assert.Nil(t, elem.Result.Errors)
	}
}

func DeleteClass(t *testing.T, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	AssertRequestOk(t, delRes, err, nil)
}
