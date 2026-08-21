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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func testMigrationSubject(version uint64, code MigrationStrategyCode, props ...string) MigrationSubject {
	subject := MigrationSubject{
		Key:                  MigrationRecordKey{TaskVersion: version, StrategyCode: code, UnitID: "shard-1__node-0"},
		TaskID:               "Books:change-tokenization:title:ab12",
		MigrationType:        ReindexTypeChangeTokenization,
		Properties:           props,
		TargetTokenization:   models.PropertyTokenizationLowercase,
		OriginalTokenization: models.PropertyTokenizationWord,
		SidecarDirs:          []string{fmt.Sprintf("m_%d_sidecar", version)},
	}
	if len(props) == 0 {
		return subject
	}

	subject.StagedDirs = map[string]string{}
	subject.CanonicalDirs = map[string]string{}
	for _, prop := range props {
		subject.StagedDirs[prop] = fmt.Sprintf("m_%d_%s", version, prop)
		subject.CanonicalDirs[prop] = "property_" + prop
	}
	subject.DisplacementLinks = map[string][]MigrationRecordKey{
		props[0]: {{TaskVersion: version - 1, StrategyCode: code, UnitID: "shard-1__node-0"}},
	}
	return subject
}

func TestMigrationRecordRoundTrip(t *testing.T) {
	checkpoint := MigrationCheckpoint{
		LastProcessedKey: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		ProcessedCount:   1200,
		IndexedCount:     980,
		UpdatedAt:        time.Date(2026, 8, 21, 10, 0, 0, 123456789, time.UTC),
	}
	displaced := map[string]string{"title": "property_title"}

	tests := []struct {
		name      string
		record    MigrationRecord
		wantState MigrationState
	}{
		{
			name:      "iterating carries the checkpoint",
			record:    NewMigrationRecordIterating(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"), checkpoint),
			wantState: MigrationStateIterating,
		},
		{
			name:      "iterated",
			record:    NewMigrationRecordIterated(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")),
			wantState: MigrationStateIterated,
		},
		{
			name:      "merged",
			record:    NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body")),
			wantState: MigrationStateMerged,
		},
		{
			name:      "swapped carries the flip set and the displaced handles",
			record:    NewMigrationRecordSwapped(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"), []string{"title"}, displaced),
			wantState: MigrationStateSwapped,
		},
		{
			name:      "promoted keeps the flip block so a partly failed retirement is still attributable",
			record:    NewMigrationRecordPromoted(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"), []string{"title"}, displaced),
			wantState: MigrationStatePromoted,
		},
		{
			name:      "class-level migration with no properties keeps its nil maps nil",
			record:    NewMigrationRecordMerged(testMigrationSubject(7, StrategyCodeSearchableMapToBlockmax)),
			wantState: MigrationStateMerged,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeMigrationRecord(tt.record)
			require.NoError(t, err)
			require.True(t, bytes.Contains(encoded, []byte("\n  ")), "records are indented so an operator can read one with cat")

			decoded, err := decodeMigrationRecord(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.wantState, decoded.State())
			require.Equal(t, tt.record, decoded)
		})
	}
}

func TestMigrationRecordNotUnderstood(t *testing.T) {
	valid := func(mutate func(env map[string]any)) []byte {
		encoded, err := encodeMigrationRecord(NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeEnableFilterable, "title")))
		require.NoError(t, err)
		env := map[string]any{}
		require.NoError(t, json.Unmarshal(encoded, &env))
		mutate(env)
		out, err := json.Marshal(env)
		require.NoError(t, err)
		return out
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "not json at all", data: []byte("this is not a record")},
		{name: "truncated mid-write", data: []byte(`{"formatVersion":1,"state":"mer`)},
		{name: "empty file", data: nil},
		{
			name: "format version from a future build",
			data: valid(func(env map[string]any) { env["formatVersion"] = 99 }),
		},
		{
			name: "state this build does not know",
			data: valid(func(env map[string]any) { env["state"] = "tidied" }),
		},
		{
			name: "strategy code this build does not know",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["key"].(map[string]any)["strategyCode"] = "quantum_reindex"
			}),
		},
		{
			name: "migration type this build does not know",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["migrationType"] = "reticulate-splines"
			}),
		},
		{
			name: "task version zero",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["key"].(map[string]any)["taskVersion"] = 0
			}),
		},
		{
			name: "unit missing from the key",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["key"].(map[string]any)["unitID"] = ""
			}),
		},
		{
			name: "task ID missing",
			data: valid(func(env map[string]any) { env["subject"].(map[string]any)["taskID"] = "" }),
		},
		{
			name: "checkpoint on a state that has none",
			data: valid(func(env map[string]any) {
				env["checkpoint"] = map[string]any{"processedCount": 1}
			}),
		},
		{
			name: "flip block on a state that has none",
			data: valid(func(env map[string]any) {
				env["flip"] = map[string]any{"flipped": []string{"title"}}
			}),
		},
		{
			name: "iterating without its checkpoint",
			data: valid(func(env map[string]any) { env["state"] = string(MigrationStateIterating) }),
		},
		{
			name: "swapped without its flip block",
			data: valid(func(env map[string]any) { env["state"] = string(MigrationStateSwapped) }),
		},
		{
			name: "promoted without its flip block",
			data: valid(func(env map[string]any) { env["state"] = string(MigrationStatePromoted) }),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := decodeMigrationRecord(tt.data)
			require.Error(t, err)
			require.Nil(t, rec)
		})
	}
}

func TestMigrationRecordKey(t *testing.T) {
	tests := []struct {
		name      string
		key       MigrationRecordKey
		wantFile  string
		wantValid bool
	}{
		{
			name:      "searchable half of a change-tokenization fan-out",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "42_searchable_retokenize.json",
			wantValid: true,
		},
		{
			name:      "filterable half of the same fan-out, same task and unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeFilterableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "42_filterable_retokenize.json",
			wantValid: true,
		},
		{
			name:      "a later generation on the same strategy",
			key:       MigrationRecordKey{TaskVersion: 43, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "43_searchable_retokenize.json",
			wantValid: true,
		},
		{
			name:      "generation zero is never allocated by raft",
			key:       MigrationRecordKey{TaskVersion: 0, StrategyCode: StrategyCodeEnableSearchable, UnitID: "shard-1__node-0"},
			wantFile:  "0_enable_searchable.json",
			wantValid: false,
		},
		{
			name:      "unknown strategy code",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: "quantum_reindex", UnitID: "shard-1__node-0"},
			wantFile:  "42_quantum_reindex.json",
			wantValid: false,
		},
		{
			name:      "no unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeRebuildSearchable},
			wantFile:  "42_rebuild_searchable.json",
			wantValid: false,
		},
	}

	seen := map[string]string{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantFile, tt.key.fileName())
			require.Equal(t, tt.wantValid, tt.key.valid())

			if prev, ok := seen[tt.wantFile]; ok {
				require.Failf(t, "file name collision", "%q also names %q", tt.wantFile, prev)
			}
			seen[tt.wantFile] = tt.name
		})
	}
}

func TestMigrationRecordStore(t *testing.T) {
	merged := func(version uint64, code MigrationStrategyCode) MigrationRecord {
		return NewMigrationRecordMerged(testMigrationSubject(version, code, "title"))
	}

	tests := []struct {
		name    string
		arrange func(t *testing.T, s *MigrationRecordStore)
		assert  func(t *testing.T, s *MigrationRecordStore)
	}{
		{
			name:    "a shard that never ran a migration loads empty",
			arrange: func(t *testing.T, s *MigrationRecordStore) {},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
				require.Empty(t, s.Unreadable())
			},
		},
		{
			name: "a written record survives a reload",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				got, ok := s.Get(MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0"})
				require.True(t, ok)
				require.Equal(t, merged(42, StrategyCodeEnableFilterable), got)
			},
		},
		{
			name: "a temp file a crash left behind is swept",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), "42_enable_filterable.json.4711.tmp"), []byte("half a record"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				left, err := os.ReadDir(s.Dir())
				require.NoError(t, err)
				require.Empty(t, left)
				require.Empty(t, s.Unreadable())
			},
		},
		{
			name: "a file this build cannot place is surfaced, not deleted",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Unreadable(), 1)
				require.Equal(t, "99_enable_searchable.json", s.Unreadable()[0].FileName)
				_, err := os.Stat(filepath.Join(s.Dir(), "99_enable_searchable.json"))
				require.NoError(t, err, "an unreadable record must survive the load that could not read it")
			},
		},
		{
			name: "one unreadable file does not hide the readable ones",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Records(), 1)
				require.Len(t, s.Unreadable(), 1)
			},
		},
		{
			name: "a record whose content names a different file is not trusted",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, os.Rename(
					filepath.Join(s.Dir(), "42_enable_filterable.json"),
					filepath.Join(s.Dir(), "43_enable_filterable.json")))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
				require.Len(t, s.Unreadable(), 1)
			},
		},
		{
			name: "removing a record twice is not an error",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				key := MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0"}
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, s.Remove(key))
				require.NoError(t, s.Remove(key))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
			},
		},
		{
			name: "a write racing a collection DELETE fails instead of re-creating the tree",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				// A DELETE renames the class directory away, and the shard's
				// LSM directory the store paths off goes with it.
				require.NoError(t, os.RemoveAll(filepath.Dir(filepath.Dir(s.Dir()))))
				require.Error(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				_, err := os.Stat(filepath.Dir(filepath.Dir(s.Dir())))
				require.True(t, os.IsNotExist(err),
					"the deleted collection's directory tree must stay deleted")
			},
		},
		{
			name: "records come back in ascending generation order",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(43, StrategyCodeEnableFilterable)))
				require.NoError(t, s.Put(merged(7, StrategyCodeEnableFilterable)))
				require.NoError(t, s.Put(merged(42, StrategyCodeFilterableRetokenize)))
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				var got []string
				for _, rec := range s.Records() {
					got = append(got, rec.Subject().Key.fileName())
				}
				require.Equal(t, []string{
					"7_enable_filterable.json",
					"42_enable_filterable.json",
					"42_filterable_retokenize.json",
					"43_enable_filterable.json",
				}, got)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			tt.arrange(t, store)

			// Every assertion runs against a store that re-read the directory,
			// so nothing passes on in-memory state a restart would lose.
			require.NoError(t, store.Load())
			tt.assert(t, store)
		})
	}
}

func TestMigrationRecordStoreConcurrentAccess(t *testing.T) {
	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)

	const writers, readers = 8, 8
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for gen := 1; gen <= 8; gen++ {
				subject := testMigrationSubject(uint64(i*8+gen), StrategyCodeEnableFilterable, "title")
				if err := store.Put(NewMigrationRecordMerged(subject)); err != nil {
					t.Errorf("put record: %v", err)
					return
				}
			}
		}()
	}
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 64 {
				for _, rec := range store.Records() {
					_, _ = store.Get(rec.Subject().Key)
				}
				_ = store.Unreadable()
			}
		}()
	}
	wg.Wait()

	require.NoError(t, store.Load())
	require.Len(t, store.Records(), writers*8)
}
