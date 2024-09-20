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

	cls := books.ClassOpenAIWithOptions()
	helper.CreateClass(t, cls)
	defer helper.DeleteClass(t, cls.Class)

	t.Run("update description of class", func(t *testing.T) {
		cls.Description = "updated description"
		helper.UpdateClass(t, cls)
	})
}

func Test_UpdateClassWithNamedText2VecOpenAI(t *testing.T) {
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

	cls := books.ClassNamedOpenAIWithOptions()
	helper.CreateClass(t, cls)
	defer helper.DeleteClass(t, cls.Class)

	t.Run("update description of class", func(t *testing.T) {
		cls.Description = "updated description"
		helper.UpdateClass(t, cls)
	})
}
