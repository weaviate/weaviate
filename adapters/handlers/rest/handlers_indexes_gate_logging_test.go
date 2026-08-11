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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	clientschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/backup"
)

// fixedLocalActivity is this node's slot probe answering from a fixed script.
type fixedLocalActivity struct {
	activity backup.NodeActivity
}

func (f fixedLocalActivity) Activity() backup.NodeActivity { return f.activity }

// Pins: both backup-gate refusals log the collection and property, since the
// response body omits them.
func TestBackupGateRefusalsLogCollectionAndProperty(t *testing.T) {
	running := backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1"}

	tests := []struct {
		name string
		// refuse drives the handler down one of the two refusal paths.
		refuse func(h *indexesHandlers) middleware.Responder
	}{
		{
			name: "this node's own slot",
			refuse: func(h *indexesHandlers) middleware.Responder {
				h.localBackupActivity = fixedLocalActivity{activity: running}
				return h.refuseOnLocalBackupActivity(&models.Principal{Username: "u1"}, "Movies", "title")
			},
		},
		{
			name: "a peer's slot, found by the fan-out probe",
			refuse: func(h *indexesHandlers) middleware.Responder {
				h.backupActivity = perNodeProber{activity: map[string]backup.NodeActivity{fixtureNode: running}}
				_, responder := h.probeBackupActivity(context.Background(),
					&models.Principal{Username: "u1"}, "Movies", "title")
				return responder
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := submissionHandlers(t, &raceTaskService{}, perNodeProber{})
			logger, hook := logrustest.NewNullLogger()
			h.appState.Logger = logger

			responder := tc.refuse(h)
			require.NotNil(t, responder, "a busy slot must refuse")

			var found *logrus.Entry
			for _, e := range hook.AllEntries() {
				if e.Data["action"] == "reindex_backup_gate" && e.Data["collection"] != nil {
					found = e
					break
				}
			}
			require.NotNil(t, found, "the refusal has to be traceable to the collection it refused")
			require.Equal(t, "Movies", found.Data["collection"])
			require.Equal(t, "title", found.Data["property"])
		})
	}
}

// Pins: the unsupported-probe WARN is budgeted like the other gate WARNs,
// not emitted per node per request.
func TestUnsupportedProbeWarnIsBudgeted(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	nodes := []string{"node1", "node2"}
	prober := perNodeProber{errs: map[string]error{
		"node1": clients.ErrNodeActivityUnsupported,
		"node2": clients.ErrNodeActivityUnsupported,
	}}
	budget := &gateWarnBudget{}

	for range 3 {
		scan := scanBackupActivity(context.Background(), nodes, prober, logger, budget)
		require.Empty(t, scan.UnreachableNode, "an old peer is a deliberate fail-open, not an outage")
	}

	warnings := 0
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel {
			warnings++
		}
	}
	require.Equal(t, 1, warnings, "6 unsupported answers must cost one line, not six")
}

// stubClientResponse is the generated reader's view of an HTTP response.
type stubClientResponse struct {
	code int
	body io.Reader
}

func (s stubClientResponse) Code() int                  { return s.code }
func (s stubClientResponse) Message() string            { return http.StatusText(s.code) }
func (s stubClientResponse) GetHeader(string) string    { return "" }
func (s stubClientResponse) GetHeaders(string) []string { return nil }
func (s stubClientResponse) Body() io.ReadCloser        { return io.NopCloser(s.body) }

// Pins: the generated client parses the cap's 429 as 429, not the
// undeclared-status default arm.
func TestGeneratedClientParsesTheCapRefusal(t *testing.T) {
	rec := httptest.NewRecorder()
	reindexCapExceededResponder(&models.Principal{Username: "u1"}, "Movies", 32, 32).
		WriteResponse(rec, runtime.JSONProducer())
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	reader := &clientschema.SchemaObjectsIndexesUpdateReader{}
	_, err := reader.ReadResponse(stubClientResponse{code: rec.Code, body: rec.Body}, runtime.JSONConsumer())

	var capped *clientschema.SchemaObjectsIndexesUpdateTooManyRequests
	require.ErrorAs(t, err, &capped, "the client must recognize 429, not fall through to the default arm")
	require.Len(t, capped.Payload.Error, 1)
	require.Contains(t, capped.Payload.Error[0].Message, "GET /v1/schema/Movies/indexes",
		"the actionable half of the refusal has to survive the round trip")
}
