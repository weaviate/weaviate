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

package reindex_singlenode

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"

	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// testDeleteClassBlockedByLiveReindex drives the operator's full exit path
// for a collection that has a live reindex on it:
//
//	DELETE  -> refused, naming cancel as the way out
//	cancel  -> 202
//	DELETE  -> succeeds
//
// The refusal is what stops a delete from orphaning a live task. Task
// payloads identify their collection by NAME only, so an orphan left in
// cluster state would be adopted by any later collection created with the
// same name and have its migration run against unrelated data.
//
// No existing suite pinned this path, which is how a regression that
// silently dropped the guard reached review.
func testDeleteClassBlockedByLiveReindex(t *testing.T, restURI string) {
	className := "DeleteClassGuard"
	trueVal, falseVal := true, false

	helper.SetupClient(restURI)
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: &trueVal, IndexSearchable: &trueVal},
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &falseVal},
		},
		Vectorizer: "none",
	})
	classDeleted := false
	defer func() {
		if !classDeleted {
			helper.DeleteClass(t, className)
		}
	}()

	// Enough objects that enable-filterable stays live long enough to
	// observe the guard.
	const n = 3000
	for i := 0; i < n; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      className,
			Properties: map[string]interface{}{"name": fmt.Sprintf("name_%d", i), "score": i},
		}))
	}

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"filterable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID)

	t.Run("DeleteIsRefusedWhileTheReindexIsLive", func(t *testing.T) {
		resp, err := helper.DeleteClassObject(t, className)
		require.Error(t, err,
			"deleting a collection with a live reindex must be refused, not silently accepted")
		require.Nil(t, resp)

		// The generated client stringifies its payload as a pointer, so
		// read the message off the typed error instead.
		badRequest := &schema.SchemaObjectsDeleteBadRequest{}
		require.ErrorAs(t, err, &badRequest)
		require.NotNil(t, badRequest.Payload)
		require.NotEmpty(t, badRequest.Payload.Error)
		msg := badRequest.Payload.Error[0].Message
		require.Contains(t, msg, "cancel",
			"the refusal must tell the operator how to get out: %s", msg)
		require.Contains(t, msg, className,
			"the refusal must name the collection: %s", msg)
	})

	t.Run("CancelThenDeleteSucceeds", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, "score")
		req, err := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"filterable":{"cancel":true}}`)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusAccepted, resp.StatusCode,
			"cancel is the documented exit and must be accepted")

		// The delete may need a moment for the cancel to reach terminal
		// state cluster-wide; the guard only clears once it does.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, delErr := helper.DeleteClassObject(t, className)
			assert.NoError(c, delErr)
		}, 60*time.Second, 500*time.Millisecond,
			"after cancelling, the collection must be deletable")
		classDeleted = true
	})
}
