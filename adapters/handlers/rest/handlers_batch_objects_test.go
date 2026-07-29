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
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/objects"
)

// Pins that referencesResponse echoes From/To on every row regardless of
// ref.Err — clients correlate failures back to the input by beacon, not
// by slice index (the slice can be reordered upstream).
func TestReferencesResponse_FailedRowsCarryBeacons(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"
	from := crossref.NewSource(schema.ClassName("Zoo"), "hasAnimals", strfmt.UUID(uuid))
	to := &crossref.Ref{
		Local:    true,
		PeerName: "localhost",
		Class:    "Animal",
		TargetID: strfmt.UUID(uuid),
	}

	input := objects.BatchReferences{
		{From: from, To: to}, // succeeds
		{From: from, To: to, Err: errors.New("validation: ref target missing")}, // fails
	}

	h := &batchObjectHandlers{}
	got := h.referencesResponse(nil, input)

	require.Len(t, got, 2)

	// Both rows must carry beacons.
	assert.NotEmpty(t, got[0].From, "success row must carry From beacon")
	assert.NotEmpty(t, got[0].To, "success row must carry To beacon")
	assert.NotEmpty(t, got[1].From, "FAILED row must still carry From beacon for correlation")
	assert.NotEmpty(t, got[1].To, "FAILED row must still carry To beacon for correlation")

	// Statuses are as expected.
	require.NotNil(t, got[0].Result.Status)
	require.NotNil(t, got[1].Result.Status)
	assert.Equal(t, models.BatchReferenceResponseAO1ResultStatusSUCCESS, *got[0].Result.Status)
	assert.Equal(t, models.BatchReferenceResponseAO1ResultStatusFAILED, *got[1].Result.Status)
	assert.Nil(t, got[0].Result.Errors)
	assert.NotNil(t, got[1].Result.Errors)
}

// Pins that nil From/To on a failed row don't panic — the strip helpers
// must tolerate nil inputs (return ""). This covers the rejection path
// where upstream fails before populating From/To.
func TestReferencesResponse_FailedRowWithNilBeaconsDoesNotPanic(t *testing.T) {
	input := objects.BatchReferences{
		{Err: errors.New("rejected before From/To set")},
	}

	h := &batchObjectHandlers{}
	got := h.referencesResponse(nil, input)

	require.Len(t, got, 1)
	require.NotNil(t, got[0].Result.Status)
	assert.Equal(t, models.BatchReferenceResponseAO1ResultStatusFAILED, *got[0].Result.Status)
	// Empty beacons are acceptable here — what matters is no panic and the
	// failure is preserved in Result.Errors.
	assert.NotNil(t, got[0].Result.Errors)
}
