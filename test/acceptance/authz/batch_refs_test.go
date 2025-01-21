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

package authz

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAuthZBatchRefAuthZCalls(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)

	compose, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{}, nil)
	defer down()

	containers := compose.Containers()
	require.Len(t, containers, 1) // started only one node

	// helper.SetupClient("127.0.0.1:8081")

	// add classes with object
	className1 := "AuthZBatchObjREST1"
	className2 := "AuthZBatchObjREST2"
	deleteObjectClass(t, className1, adminAuth)
	deleteObjectClass(t, className2, adminAuth)
	defer deleteObjectClass(t, className1, adminAuth)
	defer deleteObjectClass(t, className2, adminAuth)

	c1 := &models.Class{
		Class: className1,
		Properties: []*models.Property{
			{
				Name:     "ref",
				DataType: []string{className2},
			},
		},
	}
	c2 := &models.Class{
		Class: className2,
		Properties: []*models.Property{
			{
				Name:     "prop2",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	require.Nil(t, createClass(t, c2, adminAuth))
	require.Nil(t, createClass(t, c1, adminAuth))

	// add an object to each class
	helper.CreateObjectAuth(t, &models.Object{ID: UUID1, Class: className1}, adminKey)
	helper.CreateObjectAuth(t, &models.Object{ID: UUID1, Class: className2}, adminKey)

	ls := newLogScanner(containers[0].Container())
	ls.GetAuthzLogs(t) // startup and object class creation logs that are irrelevant

	from := beaconStart + className1 + "/" + UUID1.String() + "/ref"
	to := beaconStart + UUID1

	var refs []*models.BatchReference
	for i := 0; i < 30; i++ {
		refs = append(refs, &models.BatchReference{
			From: strfmt.URI(from), To: strfmt.URI(to),
		})
	}

	params := batch.NewBatchReferencesCreateParams().WithBody(refs)
	res, err := helper.Client(t).Batch.BatchReferencesCreate(params, adminAuth)
	require.NoError(t, err)
	require.NotNil(t, res.Payload)

	authZlogs := ls.GetAuthzLogs(t)
	require.LessOrEqual(t, len(authZlogs), 4)
}
