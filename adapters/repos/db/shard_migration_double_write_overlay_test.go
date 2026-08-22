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
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestMirrorAnalyzesPerArmedMigration covers two migrations mirroring one
// property while they disagree about how it is analyzed. That is a steady
// state: a failed generation stays armed until its successor flips, and the
// overlap check only blocks active tasks.
//
// One analysis for both is wrong in two ways, and both are silent. Same
// family, different tokenization: the older migration's staged copy accrues
// the newer one's terms, and a later promote serves them. Different family:
// the winner's overlay omits the loser's force flag, the analysis never
// produces the property in the loser's form, and the older copy stops
// receiving writes entirely.
func TestMirrorAnalyzesPerArmedMigration(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name        string
		older       inverted.PropertyOverlay
		newer       inverted.PropertyOverlay
		wantOlder   []string
		wantNewer   []string
		wantOlderIx func(inverted.Property) bool
		wantNewerIx func(inverted.Property) bool
	}{
		{
			name:      "the two agree, so one analysis is every mirror's answer",
			older:     inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord},
			newer:     inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord},
			wantOlder: []string{"alpha", "beta"},
			wantNewer: []string{"alpha", "beta"},
		},
		{
			name:      "same family, different tokenization",
			older:     inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord},
			newer:     inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationField},
			wantOlder: []string{"alpha", "beta"},
			wantNewer: []string{"Alpha Beta"},
		},
		{
			name:      "the older arm is the one with the finer tokenization",
			older:     inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationField},
			newer:     inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord},
			wantOlder: []string{"Alpha Beta"},
			wantNewer: []string{"alpha", "beta"},
		},
		{
			// Cross-family: neither force flag survives into the other's
			// analysis, so a single one leaves the loser with a property it
			// cannot write.
			name:        "different families",
			older:       inverted.PropertyOverlay{ForceFilterable: true},
			newer:       inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord},
			wantOlder:   []string{"alpha", "beta"},
			wantNewer:   []string{"alpha", "beta"},
			wantOlderIx: func(p inverted.Property) bool { return p.HasFilterableIndex },
			wantNewerIx: func(p inverted.Property) bool { return p.HasSearchableIndex },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "OverlayDivergence_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			// Neither index is on in the schema, so every posting the mirrors
			// see comes from an overlay rather than from the live property.
			for _, prop := range class.Properties {
				prop.IndexFilterable = boolPtr(false)
				prop.IndexSearchable = boolPtr(false)
			}
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			older := armRecordingMirror(shard, propName, tc.older)
			newer := armRecordingMirror(shard, propName, tc.newer)

			obj := storobj.FromObject(&models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: map[string]any{propName: "Alpha Beta"},
			}, nil, nil, nil)

			st := shard.loadPropValueIndexState()
			require.NoError(t, shard.mirrorAddToIngest(st, 1, obj))
			require.Equal(t, tc.wantOlder, older.terms(), "the older mirror's terms")
			require.Equal(t, tc.wantNewer, newer.terms(), "the newer mirror's terms")

			// The delete leg has to analyze the same way, or a term the mirror
			// wrote is never the term it removes.
			older.reset()
			newer.reset()
			require.NoError(t, shard.mirrorDeleteFromIngest(st, 1, obj))
			require.Equal(t, tc.wantOlder, older.terms(), "the older mirror's deleted terms")
			require.Equal(t, tc.wantNewer, newer.terms(), "the newer mirror's deleted terms")

			if tc.wantOlderIx != nil {
				require.True(t, tc.wantOlderIx(older.props[0]),
					"the older mirror must receive the property in the form its own overlay forces")
				require.True(t, tc.wantNewerIx(newer.props[0]),
					"the newer mirror must receive the property in the form its own overlay forces")
			}
		})
	}
}

// recordingMirror is one armed registration whose callbacks keep what they
// were handed instead of writing it.
type recordingMirror struct {
	props []inverted.Property
}

func (m *recordingMirror) reset() { m.props = nil }

// terms are sorted: the analyzer walks a map, so the order it produces them
// in says nothing about which analysis produced them.
func (m *recordingMirror) terms() []string {
	var out []string
	for _, prop := range m.props {
		for _, item := range prop.Items {
			out = append(out, string(item.Data))
		}
	}
	sort.Strings(out)
	return out
}

func armRecordingMirror(shard *Shard, propName string, overlay inverted.PropertyOverlay) *recordingMirror {
	m := &recordingMirror{}
	shard.registerDoubleWriteWithScope([]string{propName},
		map[string]inverted.PropertyOverlay{propName: overlay},
		func(scope map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
			record := func(_ *Shard, _ uint64, property *inverted.Property) error {
				if _, ok := scope[property.Name]; !ok {
					return nil
				}
				m.props = append(m.props, *property)
				return nil
			}
			return record, record
		})
	return m
}
