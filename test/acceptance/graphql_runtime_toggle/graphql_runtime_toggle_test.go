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

package graphql_runtime_toggle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	gql "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	runtimeOverridePath = "/etc/weaviate/runtime-overrides.yaml"
	pollTimeout         = 20 * time.Second
	pollInterval        = 500 * time.Millisecond
)

func writeRuntimeOverride(t *testing.T, ctx context.Context, container testcontainers.Container, contents string) {
	t.Helper()
	exitCode, _, err := container.Exec(ctx, []string{
		"sh", "-c", fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", runtimeOverridePath, contents),
	})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
}

// queryClass runs a trivial Get for className and classifies the response:
// disabled = "graphql api is disabled" (422); ok = resolved with no errors (the
// class is in the live graph); neither = a GraphQL error such as a stale graph
// missing the class.
func queryClass(t *testing.T, className string) (disabled, ok bool, detail string) {
	t.Helper()
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{
		Query: fmt.Sprintf("{ Get { %s { _additional { id } } } }", className),
	})
	resp, err := helper.Client(t).Graphql.GraphqlPost(params, nil)
	if err != nil {
		var ue *gql.GraphqlPostUnprocessableEntity
		if errors.As(err, &ue) && ue.Payload != nil && len(ue.Payload.Error) > 0 {
			msg := ue.Payload.Error[0].Message
			return strings.Contains(msg, "graphql api is disabled"), false, msg
		}
		return false, false, err.Error()
	}
	if resp.Payload != nil && len(resp.Payload.Errors) > 0 {
		return false, false, resp.Payload.Errors[0].Message
	}
	return false, true, ""
}

func eventually(t *testing.T, msg string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	var last string
	for time.Now().Before(deadline) {
		ok, detail := cond()
		if ok {
			return
		}
		last = detail
		time.Sleep(pollInterval)
	}
	t.Fatalf("%s (last observed: %s)", msg, last)
}

// TestGraphQLDisableRuntimeToggle pins the headline journey: a collection created
// while GraphQL is disabled must be queryable as soon as it is re-enabled, with no
// restart. This holds only because the schema graph is kept current while disabled
// (serving is gated, building is not) — otherwise the re-enabled query hits a stale
// "Cannot query field" error.
func TestGraphQLDisableRuntimeToggle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: runtimeOverridePath,
		FileMode:          0o644,
	}

	// Overrides reload every 1s so a toggle takes effect quickly.
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", runtimeOverridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(emptyOverride).
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	container := compose.GetWeaviateNode(1).Container()

	const colAlpha = "ColAlpha"
	const colBeta = "ColBeta"
	newClass := func(name string) *models.Class {
		return &models.Class{
			Class:      name,
			Vectorizer: "none",
			Properties: []*models.Property{
				{Name: "content", DataType: schema.DataTypeText.PropString()},
			},
		}
	}

	helper.DeleteClass(t, colAlpha)
	helper.DeleteClass(t, colBeta)
	defer helper.DeleteClass(t, colAlpha)
	defer helper.DeleteClass(t, colBeta)

	t.Run("baseline: GraphQL enabled serves an existing collection", func(t *testing.T) {
		helper.CreateClass(t, newClass(colAlpha))
		eventually(t, "ColAlpha should be queryable while GraphQL is enabled", func() (bool, string) {
			disabled, ok, detail := queryClass(t, colAlpha)
			require.False(t, disabled, "GraphQL unexpectedly disabled at baseline")
			return ok, detail
		})
	})

	t.Run("disable at runtime: GraphQL rejects queries", func(t *testing.T) {
		writeRuntimeOverride(t, ctx, container, "disable_graphql: true\n")
		eventually(t, "GraphQL should report disabled after the runtime override", func() (bool, string) {
			disabled, _, detail := queryClass(t, colAlpha)
			return disabled, detail
		})
	})

	t.Run("add a collection while disabled, then re-enable: it is queryable", func(t *testing.T) {
		// Created during the disabled window: the schema-update path must still
		// rebuild the graph so this class is present after re-enabling.
		helper.CreateClass(t, newClass(colBeta))

		writeRuntimeOverride(t, ctx, container, "disable_graphql: false\n")
		eventually(t, "ColBeta (added while disabled) should be queryable after re-enabling", func() (bool, string) {
			disabled, ok, detail := queryClass(t, colBeta)
			if disabled {
				return false, "still disabled"
			}
			return ok, detail
		})

		_, ok, detail := queryClass(t, colAlpha)
		require.True(t, ok, "ColAlpha should still be queryable after re-enabling: %s", detail)
	})

	t.Run("disable again at runtime: GraphQL rejects queries once more", func(t *testing.T) {
		writeRuntimeOverride(t, ctx, container, "disable_graphql: true\n")
		eventually(t, "GraphQL should report disabled after toggling off again", func() (bool, string) {
			disabled, _, detail := queryClass(t, colBeta)
			return disabled, detail
		})
	})
}
