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

package rest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
)

// fakeStaleCleaner records the index types it was asked to scrub and returns a
// canned error.
type fakeStaleCleaner struct {
	calls []string
	err   error
}

func (f *fakeStaleCleaner) CleanStalePartialReindexState(_ context.Context, _, _, indexType string) error {
	f.calls = append(f.calls, indexType)
	return f.err
}

// TestCleanStalePartialStateOrFail pins that the pre-submit stale-state scrub
// fails closed on an unknown migration type or a scrub error, and otherwise
// cleans every index type the migration touches before proceeding.
func TestCleanStalePartialStateOrFail(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	h := &indexesHandlers{appState: &state.State{Logger: logger}}

	t.Run("unknown migration type fails closed with 500 and scrubs nothing", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", db.ReindexMigrationType("not-a-real-type"))
		code, _ := statusOf(t, resp)
		require.Equal(t, http.StatusInternalServerError, code,
			"an unknown migration type must fail closed, not silently skip cleanup")
		require.Empty(t, cleaner.calls, "an unknown type must refuse before any scrub")
	})

	t.Run("scrub error fails closed with 500", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{err: errors.New("disk unavailable")}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", db.ReindexTypeEnableFilterable)
		code, _ := statusOf(t, resp)
		require.Equal(t, http.StatusInternalServerError, code)
	})

	t.Run("change-tokenization scrubs BOTH searchable and filterable then proceeds", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", db.ReindexTypeChangeTokenization)
		require.Nil(t, resp, "a clean scrub returns nil to proceed")
		require.Equal(t, []string{"searchable", "filterable"}, cleaner.calls,
			"the coupled migration must scrub both inverted index dirs")
	})
}
