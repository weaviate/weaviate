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

package db

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins: double-write target phase must error, not silently skip, when
// neither the sidecar nor canonical fallback bucket resolves
// (weaviate/0-weaviate-issues#336).
func TestReindex_DoubleWriteTargetPhaseNilBucket(t *testing.T) {
	ctx := testCtx()
	className := "DoubleWriteTerminalNil_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"category"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	const (
		propName         = "category"
		missingSidecar   = "dw_terminal_missing_ingest_sidecar"
		missingCanonical = "dw_terminal_missing_canonical"
	)
	sidecarNamer := func(string) string { return missingSidecar }
	canonicalNamer := func(string) string { return missingCanonical }
	inScope := map[string]struct{}{propName: {}}

	// A non-nil fallback namer arms the target phase; nil selects backup phase.
	type callback func(*Shard, uint64, *inverted.Property) error

	tests := []struct {
		name            string
		add             callback
		del             callback
		wantErrContains []string // non-empty => both callbacks must error and name these
	}{
		{
			name: "target phase, both names resolve nil, errors loudly",
			add:  callback(blockmaxSearchableAddCallback(sidecarNamer, inScope, canonicalNamer)),
			del:  callback(blockmaxSearchableDeleteCallback(sidecarNamer, inScope, canonicalNamer)),
			// class + shard locators: on CL<ALL the absorbed error is the operator's only locator.
			wantErrContains: []string{propName, missingSidecar, missingCanonical, shard.index.ID(), shard.Name()},
		},
		{
			name: "backup phase, sidecar gone, skips by design",
			add:  callback(blockmaxSearchableAddCallback(sidecarNamer, inScope, nil)),
			del:  callback(blockmaxSearchableDeleteCallback(sidecarNamer, inScope, nil)),
		},
		{
			name: "out of scope property, skips regardless of phase",
			add:  callback(blockmaxSearchableAddCallback(sidecarNamer, map[string]struct{}{}, canonicalNamer)),
			del:  callback(blockmaxSearchableDeleteCallback(sidecarNamer, map[string]struct{}{}, canonicalNamer)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prop := &inverted.Property{Name: propName}
			for legName, cb := range map[string]callback{"add": tt.add, "delete": tt.del} {
				err := cb(shard, 1, prop)
				if len(tt.wantErrContains) == 0 {
					require.NoErrorf(t, err, "%s leg must stay a silent no-op", legName)
					continue
				}
				require.Errorf(t, err, "%s leg must surface a loud error, not a silent skip", legName)
				for _, sub := range tt.wantErrContains {
					assert.Containsf(t, err.Error(), sub,
						"%s leg error must name %q so the operator can locate the state", legName, sub)
				}
			}
		})
	}
}
