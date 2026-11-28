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

package schema_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
)

func TestReindexFlip(t *testing.T) {
	retokenize := command.ReindexFlipRequest{
		Prop: "p1", Gen: 2, Tokenization: models.PropertyTokenizationField,
		IndexTypes: []string{command.ReindexFilterable, command.ReindexSearchable},
	}
	blockmax := func(prop string) command.ReindexFlipRequest {
		return command.ReindexFlipRequest{Prop: prop, Gen: 1, IndexTypes: []string{command.ReindexSearchable}, Blockmax: true}
	}
	tests := []struct {
		name          string
		flips         []command.ReindexFlipRequest
		wantLive      map[string]map[string]uint64
		wantVersion   uint64
		wantTok       string
		wantStamped   []string
		wantClassFlag bool
		wantFilter    bool
	}{
		{
			name: "flip writes live and the target settings", flips: []command.ReindexFlipRequest{retokenize},
			wantLive:    map[string]map[string]uint64{"p1": {command.ReindexFilterable: 2, command.ReindexSearchable: 2}},
			wantVersion: 2, wantTok: models.PropertyTokenizationField,
		},
		{
			name: "replayed flip changes nothing", flips: []command.ReindexFlipRequest{retokenize, retokenize},
			wantLive:    map[string]map[string]uint64{"p1": {command.ReindexFilterable: 2, command.ReindexSearchable: 2}},
			wantVersion: 2, wantTok: models.PropertyTokenizationField,
		},
		{
			name: "enable turns the index on",
			flips: []command.ReindexFlipRequest{{
				Prop: "p2", Gen: 1, IndexTypes: []string{command.ReindexFilterable}, Enable: true,
			}},
			wantLive: map[string]map[string]uint64{"p2": {command.ReindexFilterable: 1}}, wantVersion: 2,
			wantTok: models.PropertyTokenizationWord, wantFilter: true,
		},
		{
			name: "class flag waits for every searchable property", flips: []command.ReindexFlipRequest{blockmax("p1")},
			wantLive: map[string]map[string]uint64{"p1": {command.ReindexSearchable: 1}}, wantVersion: 2,
			wantTok: models.PropertyTokenizationWord, wantStamped: []string{"p1"},
		},
		{
			name: "class flag set once every searchable property is stamped", flips: []command.ReindexFlipRequest{blockmax("p1"), blockmax("p2")},
			wantLive:    map[string]map[string]uint64{"p1": {command.ReindexSearchable: 1}, "p2": {command.ReindexSearchable: 1}},
			wantVersion: 3, wantTok: models.PropertyTokenizationWord, wantStamped: []string{"p1", "p2"}, wantClassFlag: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := newSchemaManagerWithModules(t, fakes.NewMockSchemaExecutor())
			class := maskedTestClass(1.2, 100)
			off := false
			class.Properties[0].Tokenization = models.PropertyTokenizationWord
			class.Properties = append(class.Properties, &models.Property{
				Name: "p2", DataType: []string{"text"}, Tokenization: models.PropertyTokenizationWord,
				IndexFilterable: &off, IndexRangeFilters: &off,
			})
			require.NoError(t, sm.AddClass(addClassRequest(t, class), "node1", true, false))
			for i, flip := range tt.flips {
				req := applyRequest(t, command.ApplyRequest_TYPE_REINDEX_FLIP, className, uint64(i+2), flip)
				require.NoError(t, sm.ReindexFlip(req, true))
			}

			reader := sm.NewSchemaReader()
			require.Equal(t, tt.wantLive, reader.ClassLive(className))
			require.Equal(t, tt.wantVersion, reader.ClassInfo(className).ClassVersion)
			stored := reader.ReadOnlyClass(className)
			require.Equal(t, tt.wantClassFlag, stored.InvertedIndexConfig.UsingBlockMaxWAND)
			var stamped []string
			for _, p := range stored.Properties {
				if p.SearchableBlockmax != nil && *p.SearchableBlockmax {
					stamped = append(stamped, p.Name)
				}
			}
			require.Equal(t, tt.wantStamped, stamped)
			require.Equal(t, tt.wantTok, stored.Properties[0].Tokenization)
			require.Equal(t, tt.wantFilter, *stored.Properties[1].IndexFilterable)
		})
	}
}

// runGuard answers only the flip's run check.
type runGuard struct {
	schema.MutationGuard
	err error
}

func (g runGuard) CheckMigrationPropertyUpdate(string, string, command.MigrationRun) error {
	return g.err
}

// A flip applies only while its run is SWAPPING, so a late flip of a removed
// run leaves a class created again under the same name untouched.
func TestReindexFlipNeedsItsRun(t *testing.T) {
	flip := command.ReindexFlipRequest{
		Run:  command.MigrationRun{Namespace: "reindex", ID: "run", Version: 1},
		Prop: "p1", Gen: 1, IndexTypes: []string{command.ReindexFilterable},
	}
	tests := []struct {
		name     string
		runErr   error
		wantLive map[string]map[string]uint64
	}{
		{name: "the run is swapping", wantLive: map[string]map[string]uint64{"p1": {command.ReindexFilterable: 1}}},
		{name: "the run is gone", runErr: errors.New("no reindex run is completing")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := newSchemaManagerWithModules(t, fakes.NewMockSchemaExecutor())
			sm.SetMutationGuard(runGuard{err: tt.runErr})
			require.NoError(t, sm.AddClass(addClassRequest(t, maskedTestClass(1.2, 100)), "node1", true, false))
			require.NoError(t, sm.ReindexFlip(applyRequest(t, command.ApplyRequest_TYPE_REINDEX_FLIP, className, 2, flip), true))
			require.Equal(t, tt.wantLive, sm.NewSchemaReader().ClassLive(className))
		})
	}
}
