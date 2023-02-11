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

package acceptance_tests_with_client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestUpdates(t *testing.T) {
	ctx := context.Background()
	client := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, client.Schema().ClassDeleter().WithClassName("SomeClass").Do(ctx))
	classCreator := client.Schema().ClassCreator()
	class := models.Class{
		Class: "SomeClass",
		Properties: []*models.Property{{
			Name:     "SomeProp",
			DataType: []string{string(schema.DataTypeString)},
		}},
	}
	require.Nil(t, classCreator.WithClass(&class).Do(ctx))

	creator := client.Data().Creator()
	obj, err := creator.WithClassName("SomeClass").WithProperties(
		map[string]interface{}{"SomeProp": "SomeText"},
	).WithID("0a21ae23-3f22-4f34-b04a-199e701886ef").Do(ctx)
	require.Nil(t, err)

	updater := client.Data().Updater()
	require.Nil(t, updater.WithClassName("SomeClass").WithProperties(map[string]interface{}{"SomeProp": nil}).WithID(string(obj.Object.ID)).WithMerge().Do(ctx))

	// update should have cleared the object
	getter := client.Data().ObjectsGetter()
	objAfterUpdate, err := getter.WithID(string(obj.Object.ID)).WithClassName("SomeClass").Do(ctx)
	require.Nil(t, err)
	require.Len(t, objAfterUpdate[0].Properties, 0)
}
