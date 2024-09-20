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
		WithText2VecOpenAI().
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	helper.SetupClient(endpoint)

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
