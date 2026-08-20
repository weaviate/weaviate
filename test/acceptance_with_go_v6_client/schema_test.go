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

package acceptance_with_go_v6_client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/aggregate"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/modules/selfprovided"
)

type testCase struct {
	className1 string
	className2 string
}

func TestSchemaCasingClass(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)

	className1 := "RandomGreenCar"
	className2 := "RANDOMGreenCar"

	cases := []testCase{
		{className1: className1, className2: className1},
		{className1: className2, className2: className2},
		{className1: className1, className2: className2},
		{className1: className2, className2: className1},
	}
	for _, tt := range cases {
		t.Run(tt.className1+" "+tt.className2, func(t *testing.T) {
			c.Collections.Delete(ctx, tt.className1)
			c.Collections.Delete(ctx, tt.className2)

			handle, err := c.Collections.Create(ctx, newCollection(tt.className1))
			require.NoError(t, err)

			// try to create collection again with permuted-casing duplicate.
			// this should fail as it already exists
			_, err = c.Collections.Create(ctx, newCollection(tt.className2))
			checkDuplicateClassErrors(t, err, tt)

			// create object with both casing as collection name.
			require.NoError(t, insertEmptyObject(ctx, c, tt.className1))
			// this should fail if the 2nd collection is a non-equal permutation of the first
			err = insertEmptyObject(ctx, c, tt.className2)
			if tt.className1 != tt.className2 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			result, err := handle.Aggregate.OverAll(ctx, aggregate.OverAll{TotalCount: true})
			require.NoError(t, err)
			require.NotNil(t, result.TotalCount)
			if tt.className1 == tt.className2 {
				// two objects should have been added if the test case contains exact collection name matches
				require.EqualValues(t, 2, *result.TotalCount)
			} else {
				// otherwise, only one object should have been created, since the permuted collection name does not exist
				require.EqualValues(t, 1, *result.TotalCount)
			}

			// Regardless of whether a collection exists or not, the delete operation will always return a success
			require.NoError(t, c.Collections.Delete(ctx, tt.className1))
			require.NoError(t, c.Collections.Delete(ctx, tt.className2))
		})
	}
}

func newCollection(name string) collections.Collection {
	return collections.Collection{
		Name: name,
		Vectors: map[string]collections.VectorConfig{
			"default": {Vectorizer: selfprovided.Vectorizer},
		},
	}
}

// insertEmptyObject adds a single object without properties or vectors. Inserts
// are batched, so a rejected object is reported per-object rather than as a
// request error.
func insertEmptyObject(ctx context.Context, c *client.Client, className string) error {
	result, err := c.Collections.Use(className).Data.Insert(ctx, nil)
	if err != nil {
		return err
	}
	for id, msg := range result.Errors {
		return fmt.Errorf("insert %s: %s", id, msg)
	}
	return nil
}

func checkDuplicateClassErrors(t *testing.T, err error, tt testCase) {
	require.Error(t, err)

	var httpErr *client.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("unexpected error: %v", err)
	}

	var clientErr clientError
	require.NoError(t, json.Unmarshal([]byte(httpErr.Body), &clientErr))
	require.Len(t, clientErr.Error, 1)
	if tt.className1 == tt.className2 {
		require.Contains(t, clientErr.Error[0].Message, fmt.Sprintf("class name %s already exists", tt.className1))
	} else {
		require.Contains(t, clientErr.Error[0].Message, "class already exists")
		require.Contains(t, clientErr.Error[0].Message, fmt.Sprintf("found similar class %q", tt.className1))
	}
}

type clientError struct {
	Error []struct {
		Message string `json:"message"`
	} `json:"error"`
}
