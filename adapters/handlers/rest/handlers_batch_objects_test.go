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
	stderrors "errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	uco "github.com/weaviate/weaviate/usecases/objects"
)

func TestObjectsDeleteResponse(t *testing.T) {
	const (
		idA = strfmt.UUID("f8a8a1b6-2b6a-4a3b-9c8d-0f0e1d2c3b4a")
		idB = strfmt.UUID("2c1e6a3d-5b47-4e2a-8f31-6d9c0a7b4e15")
	)
	shardErr := stderrors.New("shard lookup failed")

	type wantObject struct {
		id       strfmt.UUID
		status   string
		errorMsg string
	}

	tests := []struct {
		name           string
		dryRun         bool
		output         string
		objects        uco.BatchSimpleObjects
		wantSuccessful int64
		wantFailed     int64
		wantObjects    []wantObject
	}{
		{
			name:           "verbose deletion",
			output:         verbosity.OutputVerbose,
			objects:        uco.BatchSimpleObjects{{UUID: idA}},
			wantSuccessful: 1,
			wantObjects:    []wantObject{{id: idA, status: models.BatchDeleteResponseResultsObjectsItems0StatusSUCCESS}},
		},
		{
			name:           "minimal deletion omits the id",
			output:         verbosity.OutputMinimal,
			objects:        uco.BatchSimpleObjects{{UUID: idA}},
			wantSuccessful: 1,
		},
		{
			name:        "verbose deletion failure",
			output:      verbosity.OutputVerbose,
			objects:     uco.BatchSimpleObjects{{UUID: idA, Err: shardErr}},
			wantFailed:  1,
			wantObjects: []wantObject{{id: idA, status: models.BatchDeleteResponseResultsObjectsItems0StatusFAILED, errorMsg: shardErr.Error()}},
		},
		{
			name:        "minimal deletion failure keeps the id",
			output:      verbosity.OutputMinimal,
			objects:     uco.BatchSimpleObjects{{UUID: idA, Err: shardErr}},
			wantFailed:  1,
			wantObjects: []wantObject{{id: idA, status: models.BatchDeleteResponseResultsObjectsItems0StatusFAILED, errorMsg: shardErr.Error()}},
		},
		{
			name:        "verbose dry run",
			dryRun:      true,
			output:      verbosity.OutputVerbose,
			objects:     uco.BatchSimpleObjects{{UUID: idA}},
			wantObjects: []wantObject{{id: idA, status: models.BatchDeleteResponseResultsObjectsItems0StatusDRYRUN}},
		},
		{
			name:    "minimal dry run omits the id",
			dryRun:  true,
			output:  verbosity.OutputMinimal,
			objects: uco.BatchSimpleObjects{{UUID: idA}},
		},
		{
			name:        "verbose dry run failure",
			dryRun:      true,
			output:      verbosity.OutputVerbose,
			objects:     uco.BatchSimpleObjects{{UUID: idA, Err: shardErr}},
			wantFailed:  1,
			wantObjects: []wantObject{{id: idA, status: models.BatchDeleteResponseResultsObjectsItems0StatusFAILED, errorMsg: shardErr.Error()}},
		},
		{
			name:        "minimal dry run failure keeps the id",
			dryRun:      true,
			output:      verbosity.OutputMinimal,
			objects:     uco.BatchSimpleObjects{{UUID: idA, Err: shardErr}},
			wantFailed:  1,
			wantObjects: []wantObject{{id: idA, status: models.BatchDeleteResponseResultsObjectsItems0StatusFAILED, errorMsg: shardErr.Error()}},
		},
		{
			name:        "dry run with one of two ids failing",
			dryRun:      true,
			output:      verbosity.OutputMinimal,
			objects:     uco.BatchSimpleObjects{{UUID: idA}, {UUID: idB, Err: shardErr}},
			wantFailed:  1,
			wantObjects: []wantObject{{id: idB, status: models.BatchDeleteResponseResultsObjectsItems0StatusFAILED, errorMsg: shardErr.Error()}},
		},
		{
			name:    "no matches",
			dryRun:  true,
			output:  verbosity.OutputMinimal,
			objects: uco.BatchSimpleObjects{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := &batchObjectHandlers{}

			res := h.objectsDeleteResponse(&uco.BatchDeleteResponse{
				Match:  &models.BatchDeleteMatch{Class: "Foo"},
				DryRun: test.dryRun,
				Output: test.output,
				Result: uco.BatchDeleteResult{Objects: test.objects},
			})

			assert.Equal(t, test.dryRun, *res.DryRun)
			assert.Equal(t, test.wantSuccessful, res.Results.Successful)
			assert.Equal(t, test.wantFailed, res.Results.Failed)

			require.Len(t, res.Results.Objects, len(test.wantObjects))
			for i, want := range test.wantObjects {
				got := res.Results.Objects[i]
				assert.Equal(t, want.id, got.ID)
				require.NotNil(t, got.Status)
				assert.Equal(t, want.status, *got.Status)
				if want.errorMsg == "" {
					assert.Nil(t, got.Errors)
					continue
				}
				require.NotNil(t, got.Errors)
				require.Len(t, got.Errors.Error, 1)
				assert.Equal(t, want.errorMsg, got.Errors.Error[0].Message)
			}
		})
	}
}
