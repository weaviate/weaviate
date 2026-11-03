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

package grpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	authErrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// A memory-guard shed must reach the client as ResourceExhausted, whatever its wrapping.
func TestTranslateTypedErrorMapsMemoryShedToResourceExhausted(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantTranslate bool
		wantCode      codes.Code
	}{
		{
			name:          "bare not enough memory sentinel",
			err:           enterrors.ErrNotEnoughMemory,
			wantTranslate: true,
			wantCode:      codes.ResourceExhausted,
		},
		{
			// the shape the batch stream's oomErr produces
			name:          "batch stream oom error",
			err:           fmt.Errorf("processing batch: %w", enterrors.ErrNotEnoughMemory),
			wantTranslate: true,
			wantCode:      codes.ResourceExhausted,
		},
		{
			// the shape the single-object usecases produce
			name:          "usecase wrapped memory shed",
			err:           fmt.Errorf("cannot process add object: %w", enterrors.ErrNotEnoughMemory),
			wantTranslate: true,
			wantCode:      codes.ResourceExhausted,
		},
		{
			// the shape the async indexing queue worker produces
			name:          "async indexing batch insert shed",
			err:           enterrors.NewNotEnoughMemory("add batch of 1000 vectors"),
			wantTranslate: true,
			wantCode:      codes.ResourceExhausted,
		},
		{
			name:          "not enough memory mappings sentinel",
			err:           fmt.Errorf("cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			wantTranslate: true,
			wantCode:      codes.ResourceExhausted,
		},
		{
			// control: the existing typed mappings must keep working
			name:          "forbidden still maps to permission denied",
			err:           authErrs.NewForbidden(&models.Principal{Username: "u"}, "create", "Articles"),
			wantTranslate: true,
			wantCode:      codes.PermissionDenied,
		},
		{
			// control: a genuine server fault must stay untranslated
			name:          "unrelated error is not translated",
			err:           fmt.Errorf("segment corrupted"),
			wantTranslate: false,
		},
		{
			name:          "nil error is not translated",
			err:           nil,
			wantTranslate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translateTypedError(tt.err)
			if !tt.wantTranslate {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got,
				"a memory-guard shed must be translated to a typed gRPC status, "+
					"otherwise it reaches the client as codes.Unknown and is "+
					"indistinguishable from a server fault")
			require.Equal(t, tt.wantCode, status.Code(got),
				"a memory-guard shed must surface as RESOURCE_EXHAUSTED, the same "+
					"backpressure signal the usage-limit shed already uses")
		})
	}
}
