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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func Test_UpdateClassWithText2VecOpenAI(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithText2VecOpenAI("", "", "").
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()
	endpoint := compose.GetWeaviate().URI()
	helper.SetupClient(endpoint)
	defer helper.ResetClient()

	t.Run("update description of legacy vectorizer class", func(t *testing.T) {
		cls := books.ClassOpenAIWithOptions()
		helper.CreateClass(t, cls)
		defer helper.DeleteClass(t, cls.Class)

		cls.Description = "updated description"
		helper.UpdateClass(t, cls)
	})

	t.Run("update description of named vectors class", func(t *testing.T) {
		cls := books.ClassNamedOpenAIWithOptions()
		helper.CreateClass(t, cls)
		defer helper.DeleteClass(t, cls.Class)

		cls.Description = "updated description"
		helper.UpdateClass(t, cls)
	})
}
