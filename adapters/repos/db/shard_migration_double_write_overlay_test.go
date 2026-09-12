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

func TestMirrorAnalyzesPerArmedMigration(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name             string
		older            map[string]inverted.PropertyOverlay
		newer            map[string]inverted.PropertyOverlay
		wantOlder        []string
		wantNewer        []string
		wantOlderIx      func(inverted.Property) bool
		wantNewerIx      func(inverted.Property) bool
		schemaSearchable bool
	}{
		{
			name:      "the two agree, so one analysis is every mirror's answer",
			older:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord}),
			newer:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord}),
			wantOlder: []string{"alpha", "beta"},
			wantNewer: []string{"alpha", "beta"},
		},
		{
			name:      "same family, different tokenization",
			older:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord}),
			newer:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationField}),
			wantOlder: []string{"alpha", "beta"},
			wantNewer: []string{"Alpha Beta"},
		},
		{
			name:      "the older arm is the one with the finer tokenization",
			older:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationField}),
			newer:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord}),
			wantOlder: []string{"Alpha Beta"},
			wantNewer: []string{"alpha", "beta"},
		},
		{
			name:        "different families",
			older:       overlayOn(propName, inverted.PropertyOverlay{ForceFilterable: true}),
			newer:       overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord}),
			wantOlder:   []string{"alpha", "beta"},
			wantNewer:   []string{"alpha", "beta"},
			wantOlderIx: func(p inverted.Property) bool { return p.HasFilterableIndex },
			wantNewerIx: func(p inverted.Property) bool { return p.HasSearchableIndex },
		},
		{
			name:      "one arm wants the schema's own analysis and the other forces an index on",
			newer:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord}),
			wantNewer: []string{"alpha", "beta"},
		},
		{
			name:      "the arm wanting the schema's own analysis is the newer one",
			older:     overlayOn(propName, inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord}),
			wantOlder: []string{"alpha", "beta"},
		},
		{
			name:             "neither arm carries an overlay",
			schemaSearchable: true,
			wantOlder:        []string{"alpha", "beta"},
			wantNewer:        []string{"alpha", "beta"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "OverlayDivergence_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			for _, prop := range class.Properties {
				prop.IndexFilterable = boolPtr(false)
				prop.IndexSearchable = boolPtr(tc.schemaSearchable)
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

type recordingMirror struct {
	props []inverted.Property
}

func (m *recordingMirror) reset() { m.props = nil }

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

func overlayOn(propName string, overlay inverted.PropertyOverlay) map[string]inverted.PropertyOverlay {
	return map[string]inverted.PropertyOverlay{propName: overlay}
}

func armRecordingMirror(shard *Shard, propName string, overlay map[string]inverted.PropertyOverlay) *recordingMirror {
	m := &recordingMirror{}
	shard.registerDoubleWriteWithScope([]string{propName}, overlay,
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
