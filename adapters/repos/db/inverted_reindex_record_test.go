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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

func testMigrationSubject(version uint64, code MigrationStrategyCode, props ...string) MigrationSubject {
	subject := MigrationSubject{
		Key:                  MigrationRecordKey{TaskVersion: version, StrategyCode: code, UnitID: "shard-1__node-0"},
		TaskID:               "Books:change-tokenization:title:ab12",
		MigrationType:        ReindexTypeChangeTokenization,
		TargetTokenization:   models.PropertyTokenizationLowercase,
		OriginalTokenization: models.PropertyTokenizationWord,
		TrackerDir:           fmt.Sprintf("m_%d_tracker", version),
		IterationCutoff:      time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC),
	}
	if len(props) == 0 {
		return subject
	}

	subject.Props = map[string]MigrationPropertyDirs{}
	for _, prop := range props {
		subject.Props[prop] = MigrationPropertyDirs{
			Staged:    fmt.Sprintf("property_%s__g%d_ingest", prop, version),
			Canonical: sourceBucketNameFor(code, prop),
			Sidecar:   fmt.Sprintf("property_%s__s%d_reindex", prop, version),
		}
	}
	return subject
}

// Rewrites one role of one property, which a struct value cannot take in place.
func setMigrationDir(subject *MigrationSubject, prop string, set func(*MigrationPropertyDirs)) {
	dirs := subject.Props[prop]
	set(&dirs)
	if subject.Props == nil {
		subject.Props = map[string]MigrationPropertyDirs{}
	}
	subject.Props[prop] = dirs
}

func TestMigrationRecordRoundTrip(t *testing.T) {
	checkpoint := MigrationCheckpoint{
		LastProcessedKey: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		UpdatedAt:        time.Date(2026, 8, 21, 10, 0, 0, 123456789, time.UTC),
	}
	displaced := map[string]string{"title": "property_title"}
	nilDirMaps := testMigrationSubject(7, StrategyCodeSearchableMapToBlockmax, "title")
	nilDirMaps.Props = map[string]MigrationPropertyDirs{"title": {}}

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
			name: "swapped carries the promotion it started, which is the only thing that makes a missing staged dir readable",
			record: NewMigrationRecordSwapped(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"),
				[]string{"title"}, displaced).WithPromotionAt("title", migrationPromotionStarted),
			wantState: MigrationStateSwapped,
		},
		{
			name:      "promoted keeps the flip block so a partly failed retirement is still attributable",
			record:    NewMigrationRecordPromoted(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"), []string{"title"}, displaced),
			wantState: MigrationStatePromoted,
		},
		{
			name:      "a record naming no directories keeps its nil maps nil",
			record:    NewMigrationRecordMerged(nilDirMaps),
			wantState: MigrationStateMerged,
		},
		{
			name: "two properties that displaced nothing still name one owner each",
			record: NewMigrationRecordSwapped(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body"),
				[]string{"title", "body"}, map[string]string{"title": "", "body": ""}),
			wantState: MigrationStateSwapped,
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

// A round trip cannot catch a renamed JSON tag; on disk it decodes at zero.
func TestTheRecordsWireNamesAreTheCompatibilityContract(t *testing.T) {
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")

	keysOf := func(t *testing.T, data []byte, path ...string) []string {
		t.Helper()
		var block map[string]any
		require.NoError(t, json.Unmarshal(data, &block))
		for _, step := range path {
			require.Contains(t, block, step)
			block = block[step].(map[string]any)
		}
		return slices.Sorted(maps.Keys(block))
	}

	swapped, err := encodeMigrationRecord(NewMigrationRecordSwapped(subject,
		[]string{"title"}, map[string]string{"title": "property_title"}).
		WithPromotionAt("title", migrationPromotionFinished))
	require.NoError(t, err)
	iterating, err := encodeMigrationRecord(NewMigrationRecordIterating(subject,
		MigrationCheckpoint{LastProcessedKey: []byte{1}}))
	require.NoError(t, err)

	require.Equal(t, []string{"flip", "formatVersion", "state", "subject"},
		keysOf(t, swapped))
	require.Equal(t, []string{
		"iterationCutoff", "key", "migrationType", "originalTokenization",
		"props", "targetTokenization", "taskID", "trackerDir",
	}, keysOf(t, swapped, "subject"))
	require.Equal(t, []string{"canonical", "sidecar", "staged"},
		keysOf(t, swapped, "subject", "props", "title"))
	require.Equal(t, []string{"strategyCode", "taskVersion", "unitID"},
		keysOf(t, swapped, "subject", "key"))
	require.Equal(t, []string{"displacedDirs", "flipped", "promotion"},
		keysOf(t, swapped, "flip"))
	require.Equal(t, []string{"lastProcessedKey", "updatedAt"},
		keysOf(t, iterating, "checkpoint"))

	codes := map[MigrationStrategyCode]string{
		StrategyCodeSearchableMapToBlockmax:     "searchable_map_to_blockmax",
		StrategyCodeFilterableRoaringsetRefresh: "filterable_roaringset_refresh",
		StrategyCodeFilterableToRangeable:       "filterable_to_rangeable",
		StrategyCodeSearchableRetokenize:        "searchable_retokenize",
		StrategyCodeFilterableRetokenize:        "filterable_retokenize",
		StrategyCodeEnableFilterable:            "enable_filterable",
		StrategyCodeEnableSearchable:            "enable_searchable",
		StrategyCodeRebuildSearchable:           "rebuild_searchable",
	}
	require.Len(t, codes, 8, "two codes now render the same string, so their records share a file name")
	for code, onDisk := range codes {
		require.Equal(t, onDisk, string(code))
	}
}

func TestAFlipKeyThisBuildDoesNotKnowStillDecodes(t *testing.T) {
	subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
	swapped := NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": "property_title"})

	encoded, err := encodeMigrationRecord(swapped)
	require.NoError(t, err)
	env := map[string]any{}
	require.NoError(t, json.Unmarshal(encoded, &env))
	env["flip"].(map[string]any)["someLaterBuildsBlock"] = map[string]any{"title": "whatever"}
	data, err := json.Marshal(env)
	require.NoError(t, err)

	decoded, err := decodeMigrationRecord(data)
	require.NoError(t, err)
	require.Equal(t, swapped, decoded)
	require.Empty(t, decoded.(MigrationRecordSwapped).PromotionOf("title"),
		"a key this build cannot read licenses no promotion")
}

func TestMigrationRecordNotUnderstood(t *testing.T) {
	propDirs := func(env map[string]any, prop string) map[string]any {
		props := env["subject"].(map[string]any)["props"].(map[string]any)
		if props[prop] == nil {
			props[prop] = map[string]any{}
		}
		return props[prop].(map[string]any)
	}

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

	type propReader func(prop string) map[string]any

	twoProperties := func(mutate func(dirs propReader, env map[string]any)) []byte {
		return valid(func(env map[string]any) {
			body := propDirs(env, "body")
			body["staged"] = "property_body__g42_ingest"
			body["canonical"] = "property_body"
			body["sidecar"] = "property_body__s42_reindex"
			mutate(func(prop string) map[string]any { return propDirs(env, prop) }, env)
		})
	}

	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name: "not json at all", data: []byte("this is not a record"),
			wantErr: "decode record:",
		},
		{
			name: "truncated mid-write", data: []byte(`{"formatVersion":1,"state":"mer`),
			wantErr: "unexpected end of JSON input",
		},
		{name: "empty file", data: nil, wantErr: "unexpected end of JSON input"},
		{
			name:    "format version from a future build",
			data:    valid(func(env map[string]any) { env["formatVersion"] = 99 }),
			wantErr: "unknown record format version 99",
		},
		{
			name:    "state this build does not know",
			data:    valid(func(env map[string]any) { env["state"] = "tidied" }),
			wantErr: "names unknown state \"tidied\"",
		},
		{
			name: "migration type this build does not know",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["migrationType"] = "reticulate-splines"
			}),
			wantErr: "names unknown migration type \"reticulate-splines\"",
		},
		{
			name:    "task ID missing",
			data:    valid(func(env map[string]any) { env["subject"].(map[string]any)["taskID"] = "" }),
			wantErr: "has no task ID",
		},
		{
			name: "checkpoint on a state that has none",
			data: valid(func(env map[string]any) {
				env["checkpoint"] = map[string]any{"processedCount": 1}
			}),
			wantErr: "in state \"merged\": checkpoint block present=true, wanted=false",
		},
		{
			name: "flip block on a state that has none",
			data: valid(func(env map[string]any) {
				env["flip"] = map[string]any{"flipped": []string{"title"}}
			}),
			wantErr: "in state \"merged\": flip block present=true, wanted=false",
		},
		{
			name:    "iterating without its checkpoint",
			data:    valid(func(env map[string]any) { env["state"] = string(MigrationStateIterating) }),
			wantErr: "in state \"iterating\": checkpoint block present=false, wanted=true",
		},
		{
			name:    "swapped without its flip block",
			data:    valid(func(env map[string]any) { env["state"] = string(MigrationStateSwapped) }),
			wantErr: "in state \"swapped\": flip block present=false, wanted=true",
		},
		{
			name:    "promoted without its flip block",
			data:    valid(func(env map[string]any) { env["state"] = string(MigrationStatePromoted) }),
			wantErr: "in state \"promoted\": flip block present=false, wanted=true",
		},
		{
			name: "a flip that displaced the directory it staged",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": propDirs(env, "title")["staged"]},
				}
			}),
			wantErr: `names directory "property_title__g42_ingest" as both the staged directory of property "title" and the displaced directory of property "title"`,
		},
		{
			name: "a flip that displaced another property's staged directory",
			data: valid(func(env map[string]any) {
				body := propDirs(env, "body")
				body["staged"] = "property_body__g42_ingest"
				body["canonical"] = "property_body"
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": body["staged"]},
				}
			}),
			wantErr: `names directory "property_body__g42_ingest" as both the staged directory of property "body" and the displaced directory of property "title"`,
		},
		{
			name: "two properties naming the same sidecar directory",
			data: valid(func(env map[string]any) {
				propDirs(env, "body")["sidecar"] = propDirs(env, "title")["sidecar"]
			}),
			wantErr: `names directory "property_title__s42_reindex" as both the sidecar directory of property "body" and the sidecar directory of property "title"`,
		},
		{
			name: "a displaced directory recorded for a property the record does not name",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"body": "property_body"},
				}
			}),
			wantErr: `displaces a directory for property "body", which it does not name`,
		},
		{
			name: "a promotion recorded for a property the record does not name",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": "property_title"},
					"promotion":     map[string]any{"body": "started"},
				}
			}),
			wantErr: `records a promotion of property "body", which it does not name`,
		},
		{
			name: "a promotion mark on a state that has no promotion to be part way through",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStatePromoted)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": "property_title"},
					"promotion":     map[string]any{"title": "finished"},
				}
			}),
			wantErr: `in state "promoted" carries promotion marks, which only a swapped record does`,
		},
		{
			name: "a promotion mark this build does not know",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": "property_title"},
					"promotion":     map[string]any{"title": "bogus"},
				}
			}),
			wantErr: `records unknown promotion mark "bogus" for property "title"`,
		},
		{
			name: "two properties naming the same staged directory",
			data: twoProperties(func(dirs propReader, _ map[string]any) {
				dirs("body")["staged"] = dirs("title")["staged"]
			}),
			wantErr: `names directory "property_title__g42_ingest" as both the staged directory of property "body" and the staged directory of property "title"`,
		},
		{
			name: "a property staged into another property's sidecar directory",
			data: twoProperties(func(dirs propReader, _ map[string]any) {
				dirs("body")["staged"] = dirs("title")["sidecar"]
			}),
			wantErr: `names directory "property_title__s42_reindex" as both the staged directory of property "body" and the sidecar directory of property "title"`,
		},
		{
			name: "a property staged into its own sidecar directory",
			data: twoProperties(func(dirs propReader, _ map[string]any) {
				dirs("title")["staged"] = dirs("title")["sidecar"]
			}),
			wantErr: `names directory "property_title__s42_reindex" as both the staged directory of property "title" and the sidecar directory of property "title"`,
		},
		{
			name: "two properties naming the same canonical directory",
			data: twoProperties(func(dirs propReader, _ map[string]any) {
				dirs("body")["canonical"] = dirs("title")["canonical"]
			}),
			wantErr: `names directory "property_title" as both the canonical directory of property "body" and the canonical directory of property "title"`,
		},
		{
			name: "a property staged into another property's canonical directory",
			data: twoProperties(func(dirs propReader, _ map[string]any) {
				dirs("title")["canonical"] = "property_title__retokenize_ingest_1"
				dirs("body")["staged"] = dirs("title")["canonical"]
			}),
			wantErr: `names directory "property_title__retokenize_ingest_1" as both the staged directory of property "body" and the canonical directory of property "title"`,
		},
		{
			name: "a sidecar that is another property's canonical directory",
			data: twoProperties(func(dirs propReader, _ map[string]any) {
				dirs("title")["canonical"] = "property_title__retokenize_ingest_1"
				dirs("body")["sidecar"] = dirs("title")["canonical"]
			}),
			wantErr: `names directory "property_title__retokenize_ingest_1" as both the canonical directory of property "title" and the sidecar directory of property "body"`,
		},
		{
			name: "a flip that displaced a sidecar directory",
			data: twoProperties(func(dirs propReader, env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": dirs("body")["sidecar"]},
				}
			}),
			wantErr: `names directory "property_body__s42_reindex" as both the sidecar directory of property "body" and the displaced directory of property "title"`,
		},
		{
			name: "a flip that displaced another property's canonical directory",
			data: twoProperties(func(dirs propReader, env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": dirs("body")["canonical"]},
				}
			}),
			wantErr: `names directory "property_body" as both the canonical directory of property "body" and the displaced directory of property "title"`,
		},
		{
			name: "two properties displacing the same directory",
			data: twoProperties(func(_ propReader, env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": "property_shared__g41_ingest", "body": "property_shared__g41_ingest"},
				}
			}),
			wantErr: `names directory "property_shared__g41_ingest" as both the displaced directory of property "body" and the displaced directory of property "title"`,
		},
		{
			// Every other role is covered by
			// [TestNoStoreTheShardServesFromCanBeNamedInAnyDirectoryRole]; the
			// tracker directory is the one that does not sit at the shard root.
			name: "a tracker directory that is the record store",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["trackerDir"] = migrationRecordsDirName
			}),
			wantErr: `names tracker directory "records", which is a directory no migration may own`,
		},
		{
			name: "a unit the record file name could not carry",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["key"].(map[string]any)["unitID"] = "../shard-2__node-0"
			}),
			wantErr: "record key \"42/enable_filterable/../shard-2__node-0\" is incomplete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := decodeMigrationRecord(tt.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
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
			wantFile:  "42_searchable_retokenize_shard-1__node-0.json",
			wantValid: true,
		},
		{
			name:      "filterable half of the same fan-out, same task and unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeFilterableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "42_filterable_retokenize_shard-1__node-0.json",
			wantValid: true,
		},
		{
			name:      "a later generation on the same strategy",
			key:       MigrationRecordKey{TaskVersion: 43, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "43_searchable_retokenize_shard-1__node-0.json",
			wantValid: true,
		},
		{
			name:      "generation zero is never allocated by raft",
			key:       MigrationRecordKey{TaskVersion: 0, StrategyCode: StrategyCodeEnableSearchable, UnitID: "shard-1__node-0"},
			wantFile:  "0_enable_searchable_shard-1__node-0.json",
			wantValid: false,
		},
		{
			name:      "unknown strategy code",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: "quantum_reindex", UnitID: "shard-1__node-0"},
			wantFile:  "42_quantum_reindex_shard-1__node-0.json",
			wantValid: false,
		},
		{
			name:      "no unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeRebuildSearchable},
			wantFile:  "42_rebuild_searchable_.json",
			wantValid: false,
		},
		{
			name:      "the same task and strategy on another unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-1"},
			wantFile:  "42_searchable_retokenize_shard-1__node-1.json",
			wantValid: true,
		},
		{
			name:      "a unit that would escape the records directory",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "../shard-2__node-0"},
			wantFile:  "42_enable_filterable_../shard-2__node-0.json",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantFile, tt.key.fileName())
			require.Equal(t, tt.wantValid, tt.key.valid())
		})
	}
}

func TestMigrationRecordStore(t *testing.T) {
	// Two strategies of one submission share its task version, and production
	// tells their directories apart by the strategy word the name carries.
	merged := func(version uint64, code MigrationStrategyCode) MigrationRecord {
		subject := testMigrationSubject(version, code, "title")
		subject.TrackerDir = fmt.Sprintf("%s_%d_tracker", code, version)
		setMigrationDir(&subject, "title", func(d *MigrationPropertyDirs) {
			d.Staged = fmt.Sprintf("property_title__%s_%d_ingest", code, version)
			d.Sidecar = fmt.Sprintf("property_title__%s_%d_reindex", code, version)
		})
		return NewMigrationRecordMerged(subject)
	}

	tests := []struct {
		name        string
		arrange     func(t *testing.T, s *MigrationRecordStore)
		assert      func(t *testing.T, s *MigrationRecordStore)
		wantLoadErr bool
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
			name: "a file this build cannot place is surfaced, not deleted",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				plantUnreadableRecord(t, s.Dir())
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
				plantUnreadableRecord(t, s.Dir())
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
					filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"),
					filepath.Join(s.Dir(), "43_enable_filterable_shard-1__node-0.json")))
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
			name: "a scratch file another writer owns survives a load and only the owner sweeps it",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), "42_enable_filterable.json.1234"+tmpExt), []byte("half"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				scratch := filepath.Join(s.Dir(), "42_enable_filterable.json.1234"+tmpExt)
				require.Empty(t, s.Records())
				require.Empty(t, s.Unreadable(), "a scratch file is not a record this build failed to read")

				logger, _ := test.NewNullLogger()
				foreign := NewMigrationRecordStore(filepath.Dir(filepath.Dir(s.Dir())), logger)
				require.NoError(t, foreign.Load())
				_, err := os.Stat(scratch)
				require.NoError(t, err, "a foreign reader must not delete a scratch file it does not own")

				s.SweepTempFiles()
				left, err := os.ReadDir(s.Dir())
				require.NoError(t, err)
				require.Empty(t, left, "the owning store sweeps what a crash left behind")
			},
		},
		{
			name: "a record this build cannot read refuses every write, not only its own key",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(
					filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"), []byte("{torn"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Unreadable(), 1)
				require.Error(t, s.Put(merged(42, StrategyCodeEnableFilterable)))

				kept, err := os.ReadFile(filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"))
				require.NoError(t, err)
				require.Equal(t, "{torn", string(kept))

				require.Error(t, s.Put(merged(43, StrategyCodeEnableFilterable)),
					"the record nobody could read may name the same directories, so no key can be decided here")
			},
		},
		{
			name: "a records directory that cannot be read leaves the whole set unreadable, not clean",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(filepath.Dir(s.Dir()), 0o777))
				require.NoError(t, os.WriteFile(s.Dir(), []byte("not a directory"), 0o600))
			},
			wantLoadErr: true,
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
				require.NotEmpty(t, s.Unreadable(),
					"a directory nobody could read must withhold, not report a clean shard")
			},
		},
		{
			name: "a records directory that could not be read freezes every write, not one file name",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, os.Rename(s.Dir(), s.Dir()+".aside"))
				require.NoError(t, os.WriteFile(s.Dir(), []byte("not a directory"), 0o600))
			},
			wantLoadErr: true,
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.Remove(s.Dir()))
				require.NoError(t, os.Rename(s.Dir()+".aside", s.Dir()))

				before, err := os.ReadFile(filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"))
				require.NoError(t, err)

				require.Error(t, s.Put(NewMigrationRecordIterating(
					testMigrationSubject(42, StrategyCodeEnableFilterable, "title"), MigrationCheckpoint{})),
					"a build that cannot tell what is recorded here must not demote a flip record")
				require.Error(t, s.Remove(MigrationRecordKey{
					TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0",
				}), "the freeze that preserves a record must not be undone by removing it")

				after, err := os.ReadFile(filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"))
				require.NoError(t, err)
				require.Equal(t, before, after)
			},
		},
		{
			name: "a file this build cannot read refuses every removal, whatever key it names",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(43, StrategyCodeEnableFilterable)))
				require.NoError(t, os.WriteFile(
					filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"), []byte("{torn"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Error(t, s.Remove(MigrationRecordKey{
					TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0",
				}), "the record this build could not read")
				require.Error(t, s.Remove(MigrationRecordKey{
					TaskVersion: 43, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0",
				}), "and the one it could: retiring it decides a shard nobody here can read")

				for _, name := range []string{
					"42_enable_filterable_shard-1__node-0.json",
					"43_enable_filterable_shard-1__node-0.json",
				} {
					require.FileExists(t, filepath.Join(s.Dir(), name))
				}
			},
		},
		{
			name: "a foreign unit's record is neither answered for nor overwritten",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				foreign := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
				foreign.Key.UnitID = "shard-1__node-9"
				require.NoError(t, s.Put(NewMigrationRecordMerged(foreign)))
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Records(), 2)
				for _, unit := range []string{"shard-1__node-0", "shard-1__node-9"} {
					_, ok := s.Get(MigrationRecordKey{
						TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: unit,
					})
					require.True(t, ok, "unit %q lost its record", unit)
				}
			},
		},
		{
			name: "two units on one shard freeze it rather than being acted on",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				foreign := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
				foreign.Key.UnitID = "shard-1__node-9"
				require.NoError(t, s.Put(NewMigrationRecordMerged(foreign)))
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Unreadable(), 1)
				require.Equal(t, MigrationRecordFaultStore, s.Unreadable()[0].Scope)
				require.Contains(t, s.Unreadable()[0].Reason, "shard-1__node-9")

				require.Error(t, s.Put(merged(43, StrategyCodeEnableFilterable)),
					"a frozen store must not take a write it cannot place among the records it could not attribute")
			},
		},
		{
			name: "records come back in ascending task-version order, then strategy code",
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
					"7_enable_filterable_shard-1__node-0.json",
					"42_enable_filterable_shard-1__node-0.json",
					"42_filterable_retokenize_shard-1__node-0.json",
					"43_enable_filterable_shard-1__node-0.json",
				}, got)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			tt.arrange(t, store)

			err := store.Load()
			if tt.wantLoadErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tt.assert(t, store)
		})
	}
}

func TestMigrationRecordStoreConcurrentAccess(t *testing.T) {
	logger, _ := test.NewNullLogger()
	lsmPath := t.TempDir()
	store := NewMigrationRecordStore(lsmPath, logger)

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
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 64 {
				_ = NewMigrationRecordStore(lsmPath, logger).Load()
			}
		}()
	}
	wg.Wait()

	require.Len(t, store.Records(), writers*8,
		"every put publishes into the map the readers were walking")
	require.NoError(t, store.Load())
	require.Len(t, store.Records(), writers*8)
}

func TestDecodeMigrationRecordRejectsEscapingHandles(t *testing.T) {
	tests := []struct {
		name   string
		place  func(*MigrationSubject, *migrationFlipEnvelope, string)
		handle string
		// A record with no flip leaves the handle check the only rule that can
		// refuse what place writes.
		noFlip    bool
		wantErr   bool
		wantField string
	}{
		{
			name:   "tracker directory",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.TrackerDir = h },
			handle: "../../../../etc", wantErr: true,
		},
		{
			name: "staged directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{"title": {Staged: h}}
			},
			handle: "/var/lib/weaviate", wantErr: true, wantField: "staged directory",
		},
		{
			name: "canonical directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{"title": {Canonical: h}}
			},
			handle: "../sibling_shard/property_title", wantErr: true, wantField: "canonical directory",
		},
		{
			name: "sidecar directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{"title": {Sidecar: h}}
			},
			handle: "..", wantErr: true, wantField: "sidecar directory",
		},
		{
			name: "displaced directory",
			place: func(_ *MigrationSubject, f *migrationFlipEnvelope, h string) {
				f.DisplacedDirs = map[string]string{"title": h}
			},
			handle: "/", wantErr: true, wantField: "displaced directory",
		},
		{
			name: "a handle that only looks like an escape stays inside",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{"title": {Staged: h}}
			},
			handle: "property_..title__g42_ingest",
		},
		{
			name: "a nested handle names no directory a writer can produce",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{"title": {Sidecar: h}}
			},
			handle: "property_tracker__g42_ingest/searchable/title", wantErr: true,
		},
		{
			name:   "an empty handle is the ordinary names-none",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.TrackerDir = h },
			handle: "",
		},
		{
			name: "the current directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{"title": {Sidecar: h}}
			},
			handle: ".", wantErr: true,
		},
		{
			name:   "a descent and an ascent that cancel",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.TrackerDir = h },
			handle: "x/..", wantErr: true,
		},
		{
			name: "a property name that escapes",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{h: {}}
			},
			handle: "x/../../../../etc", noFlip: true, wantErr: true, wantField: "property",
		},
		{
			name: "an empty property name, which composes into another property's bucket",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.Props = map[string]MigrationPropertyDirs{h: {}}
			},
			handle: "", noFlip: true, wantErr: true, wantField: "property",
		},
		{
			name: "a poisoned displaced-dirs key",
			place: func(_ *MigrationSubject, f *migrationFlipEnvelope, h string) {
				f.DisplacedDirs = map[string]string{h: "property_title__g42_ingest"}
			},
			handle: "../../evil", wantErr: true, wantField: "property",
		},
		{
			name: "a poisoned flipped-properties entry",
			place: func(_ *MigrationSubject, f *migrationFlipEnvelope, h string) {
				f.Flipped = []string{h}
			},
			handle: "../../evil", wantErr: true, wantField: "property",
		},
		{
			name: "an ordinary property name decodes",
			place: func(s *MigrationSubject, f *migrationFlipEnvelope, h string) {
				s.Props, f.Flipped = map[string]MigrationPropertyDirs{h: {}}, []string{h}
			},
			handle: "title_2",
		},
		{
			name: "a property named after the record store decodes",
			place: func(s *MigrationSubject, f *migrationFlipEnvelope, h string) {
				s.Props, f.Flipped = map[string]MigrationPropertyDirs{h: {}}, []string{h}
			},
			handle: migrationRecordsDirName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
			subject.TrackerDir = ""
			subject.Props = map[string]MigrationPropertyDirs{"title": {}}
			flip := migrationFlipEnvelope{Flipped: []string{"title"}}
			tt.place(&subject, &flip, tt.handle)

			env := migrationRecordEnvelope{
				FormatVersion: migrationRecordFormatVersion,
				State:         MigrationStateSwapped,
				Subject:       subject,
				Flip:          &flip,
			}
			if tt.noFlip {
				env.State, env.Flip = MigrationStateIterated, nil
			}
			data, err := json.Marshal(env)
			require.NoError(t, err)

			rec, err := decodeMigrationRecord(data)
			if tt.wantErr {
				require.Error(t, err, "a handle that leaves the shard root must not decode")
				require.Nil(t, rec)
				if tt.wantField != "" {
					require.Contains(t, err.Error(),
						fmt.Sprintf("names %s %q, which is not a single directory inside the shard",
							tt.wantField, tt.handle),
						"the handle check has to be the rule that refuses this, not one that names the same handle")
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, rec)
		})
	}
}

// A record only the writer accepts wedges that key until fixed by hand.
func TestTheWriterRefusesWhatTheLoaderWouldReject(t *testing.T) {
	tests := []struct {
		name    string
		mangle  func(*MigrationSubject)
		because string
		wantErr string
	}{
		{
			name:    "a tracker directory that leaves the shard root",
			mangle:  func(s *MigrationSubject) { s.TrackerDir = "../../../etc" },
			because: "the tracker directory is joined onto the shard and handed to a recursive delete",
			wantErr: "names tracker directory \"../../../etc\"",
		},
		{
			name: "a staged directory that is a live bucket of another property",
			mangle: func(s *MigrationSubject) {
				s.Props = map[string]MigrationPropertyDirs{"title": {Staged: "property_body_searchable"}}
			},
			because: "a staged handle is reclaimed on every teardown path, so a live bucket named there is deleted",
			wantErr: `names staged directory "property_body_searchable", which is not shaped like a sidecar of a property bucket`,
		},
		{
			name:    "a strategy code outside the known set",
			mangle:  func(s *MigrationSubject) { s.Key.StrategyCode = "bogus" },
			because: "the code is in the file name, so the loader refuses a file the writer chose",
			wantErr: "record key \"42/bogus/shard-1__node-0\" is incomplete or names an unknown strategy",
		},
		{
			name:    "a record over the size bound the loader enforces",
			mangle:  func(s *MigrationSubject) { padMigrationSubject(s, maxMigrationRecordBytes) },
			because: "the loader refuses a file over the bound, so the writer must not build one",
			wantErr: "bound is",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			tt.mangle(&subject)
			rec := NewMigrationRecordMerged(subject)

			_, err := encodeMigrationRecord(rec)
			require.Error(t, err, tt.because)
			require.Contains(t, err.Error(), tt.wantErr)

			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			require.Error(t, store.Put(rec), "and Put refuses it for the same reason")
			require.Empty(t, store.Records(), "nothing a load would refuse reaches the map either")
		})
	}
}

// Guards a per-property field that grows with data instead of property count:
// the largest buildable record would then stop fitting.
func TestTheLargestRecordTheWriterCanBuildFitsTheLoadersBound(t *testing.T) {
	const maxDirEntryNameBytes = 255

	longest := func(role string, i int) string {
		s := fmt.Sprintf("%s_%d_", role, i)
		return s + strings.Repeat("x", maxDirEntryNameBytes-len(s))
	}
	longestSidecar := func(role string, i int) string {
		s := fmt.Sprintf("property_%s_%d__", role, i)
		const tail = "_ingest"
		return s + strings.Repeat("x", maxDirEntryNameBytes-len(s)-len(tail)) + tail
	}
	longestBucket := func(role string, i int) string {
		s := fmt.Sprintf("property_%s_%d_", role, i)
		return s + strings.Repeat("x", maxDirEntryNameBytes-len(s))
	}

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize)
	subject.TrackerDir = longest("tracker", 0)
	subject.Props = make(map[string]MigrationPropertyDirs, maxReindexPropertiesPerTask)
	displaced := map[string]string{}
	for i := 0; i < maxReindexPropertiesPerTask; i++ {
		prop := longest("property", i)
		subject.Props[prop] = MigrationPropertyDirs{
			Staged:    longestSidecar("staged", i),
			Canonical: sourceBucketNameFor(subject.Key.StrategyCode, prop),
			Sidecar:   longestSidecar("sidecar", i),
		}
		displaced[prop] = longestBucket("displaced", i)
	}

	swapped := NewMigrationRecordSwapped(subject, subject.Properties(), displaced)
	for _, prop := range subject.Properties() {
		swapped = swapped.WithPromotionAt(prop, migrationPromotionFinished)
	}

	iterating := NewMigrationRecordIterating(subject, MigrationCheckpoint{
		LastProcessedKey: bytes.Repeat([]byte{0xff}, 16),
	})

	for _, rec := range []MigrationRecord{swapped, iterating} {
		t.Run(string(rec.State()), func(t *testing.T) {
			data, err := encodeMigrationRecord(rec)
			require.NoError(t, err)
			require.Less(t, len(data), maxMigrationRecordBytes,
				"the writer must not be able to build a record the loader refuses")

			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			require.NoError(t, store.Put(rec))
			require.NoError(t, store.Load())
			require.Len(t, store.Records(), 1)
			require.Empty(t, store.Unreadable())
		})
	}
}

func TestARecordNamingNoPropertiesIsRefusedByTheWriterAndToleratedByTheLoader(t *testing.T) {
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize)
	rec := NewMigrationRecordMerged(subject)

	_, err := encodeMigrationRecord(rec)
	require.ErrorContains(t, err, "names no properties")

	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	require.Error(t, store.Put(rec), "and Put refuses it for the same reason")

	writeRawMigrationRecord(t, store, rec.toEnvelope())

	require.NoError(t, store.Load())
	require.Len(t, store.Records(), 1, "the loader reads it")
	require.Empty(t, store.Unreadable(), "and withholds nothing on its account")
}

func storeLinesAt(hook *test.Hook, level logrus.Level) []string {
	var out []string
	for _, entry := range hook.AllEntries() {
		if entry.Level == level {
			out = append(out, entry.Message)
		}
	}
	return out
}

// Load runs on every shard load, which on a multi-tenant collection is once
// per tenant, and the number of files it cannot read is not bounded by
// anything this node controls.
func TestALoadWithManyUnreadableRecordsReportsOneLine(t *testing.T) {
	const over = maxReportedErrors + 2

	logger, hook := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	require.NoError(t, os.MkdirAll(store.Dir(), 0o777))
	for i := 0; i < over; i++ {
		name := fmt.Sprintf("bad_%02d.mig", i)
		require.NoError(t, os.WriteFile(filepath.Join(store.Dir(), name), []byte("not a record"), 0o600))
	}

	require.NoError(t, store.Load())
	require.Len(t, store.Unreadable(), over)

	lines := storeLinesAt(hook, logrus.ErrorLevel)
	require.Len(t, lines, 1, "one line per load, not one per file")
	require.Contains(t, lines[0], "(and 2 more)", "and the files it names are capped")
}

// The freeze notice quotes the reason a decode gave back, and a decode refusal
// quotes the handle it refused. A record file is bounded only by the 8 MiB read
// cap, and the notice re-emits on every load of the shard.
func TestAnUnreadableRecordsReasonIsBounded(t *testing.T) {
	logger, hook := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	require.NoError(t, os.MkdirAll(store.Dir(), 0o777))

	huge := migrationPropertyBucketPrefix + strings.Repeat("x", 1<<16)
	subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
	setMigrationDir(&subject, "title", func(d *MigrationPropertyDirs) { d.Staged = huge })
	data, err := json.Marshal(newMigrationRecordEnvelope(MigrationStateMerged, subject))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(store.Dir(), subject.Key.fileName()), data, 0o600))

	require.NoError(t, store.Load())

	require.Len(t, store.Unreadable(), 1, "fixture: the handle has to be one this build refuses")
	reason := store.Unreadable()[0].Reason
	require.Less(t, len(reason), maxMigrationFaultReasonBytes+32)
	require.Contains(t, reason, "names staged directory",
		"the bound must keep the part of the reason that says which record and which role")
	require.Contains(t, reason, "not shaped like a sidecar",
		"and the part that says what is wrong with it")

	lines := storeLinesAt(hook, logrus.ErrorLevel)
	require.Len(t, lines, 1)
	require.Less(t, len(lines[0]), 4096, "so the line an operator's log holds is bounded too")
}

// The sweep runs once per shard load too, over a directory whose temp files
// are one per interrupted write.
func TestSweepingTempFilesItCannotRemoveReportsOneLine(t *testing.T) {
	logger, hook := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	require.NoError(t, os.MkdirAll(store.Dir(), 0o777))
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("rec_%d.mig%s", i, tmpExt)
		require.NoError(t, os.WriteFile(filepath.Join(store.Dir(), name), []byte("x"), 0o600))
	}
	denyDirectoryWrites(t, store.Dir())

	store.SweepTempFiles()

	lines := storeLinesAt(hook, logrus.WarnLevel)
	require.Len(t, lines, 1, "one line per sweep, not one per file")
	require.Contains(t, lines[0], "2 stale migration record temp file(s)")
}

func TestEveryPropertyBucketCarriesTheMigrationPrefix(t *testing.T) {
	for _, bucket := range []string{
		helpers.BucketFromPropNameLSM("title"),
		helpers.BucketSearchableFromPropNameLSM("title"),
		helpers.BucketRangeableFromPropNameLSM("title"),
	} {
		require.True(t, strings.HasPrefix(bucket, migrationPropertyBucketPrefix), bucket)
		require.False(t, migrationHandleIsSidecarShaped(bucket),
			"a property's own bucket must never pass as a migration's staged copy")
	}
}

// Names come from the helpers, not a literal list, so a store added later fails.
func TestNoStoreTheShardServesFromCanBeNamedInAnyDirectoryRole(t *testing.T) {
	stores := []string{
		helpers.ObjectsBucketLSM,
		helpers.DimensionsBucketLSM,
		migrationsDir,
		migrationRecordsDirName,
	}
	for _, targetVector := range []string{"", "myNamedVector"} {
		stores = append(stores, helpers.VectorIndexArtifactsFor(targetVector, nil).LSMBuckets...)
	}

	liveBuckets := []string{
		helpers.BucketFromPropNameLSM("title"),
		helpers.BucketSearchableFromPropNameLSM("title"),
		helpers.BucketRangeableFromPropNameLSM("title"),
	}

	for _, role := range migrationShardRootDirectoryRoles(t) {
		refused := stores
		if role.shape == migrationShapeSidecar {
			refused = slices.Concat(stores, liveBuckets)
		}
		for _, store := range refused {
			t.Run(role.field+"/"+store, func(t *testing.T) {
				env := migrationRecordEnvelope{
					Subject:    testMigrationSubject(1, StrategyCodeSearchableRetokenize, "title"),
					State:      MigrationStateIterating,
					Checkpoint: &MigrationCheckpoint{},
				}
				migrationDirectoryRolePlacers[role.field](&env, "title", store)
				err := validateMigrationHandles(env)
				require.Errorf(t, err, "a record naming this as its %s hands it to os.RemoveAll", role.field)
				require.Contains(t, err.Error(), store)
			})
		}
	}
}

var migrationDirectoryRolePlacers = map[string]func(env *migrationRecordEnvelope, prop, handle string){
	"staged directory": func(env *migrationRecordEnvelope, prop, handle string) {
		setMigrationDir(&env.Subject, prop, func(d *MigrationPropertyDirs) { d.Staged = handle })
	},
	"sidecar directory": func(env *migrationRecordEnvelope, prop, handle string) {
		setMigrationDir(&env.Subject, prop, func(d *MigrationPropertyDirs) { d.Sidecar = handle })
	},
	"canonical directory": func(env *migrationRecordEnvelope, prop, handle string) {
		setMigrationDir(&env.Subject, prop, func(d *MigrationPropertyDirs) { d.Canonical = handle })
	},
	"displaced directory": func(env *migrationRecordEnvelope, prop, handle string) {
		env.Flip = &migrationFlipEnvelope{
			Flipped:       []string{prop},
			DisplacedDirs: map[string]string{prop: handle},
		}
	},
}

func migrationShardRootDirectoryRoles(t *testing.T) []migrationHandleGroup {
	t.Helper()
	var out []migrationHandleGroup
	for _, group := range migrationHandleGroups {
		if !group.namesDirectory || group.underMigrationsDir {
			continue
		}
		require.Containsf(t, migrationDirectoryRolePlacers, group.field,
			"the %s role has no placer here, so no test below covers it", group.field)
		out = append(out, group)
	}
	return out
}

func TestEveryDirectoryRoleUnderTheShardRootCarriesAShapeRule(t *testing.T) {
	for _, group := range migrationShardRootDirectoryRoles(t) {
		t.Run(group.field, func(t *testing.T) {
			require.Containsf(t,
				[]migrationHandleShape{migrationShapeSidecar, migrationShapePropertyBucket},
				group.shape,
				"the %s role reaches os.RemoveAll in the shard's LSM directory, so its handle has to take either the sidecar shape or the property-bucket rule",
				group.field)
		})
	}
}

func TestEveryWriterEmittedSidecarNameIsAccepted(t *testing.T) {
	props := []string{"title", "a__b", "x_ingest", "a__reindex"}

	strategiesFor := func(prop string, generation int) []MigrationStrategy {
		return []MigrationStrategy{
			&MapToBlockmaxStrategy{generation: generation},
			&RoaringSetRefreshStrategy{generation: generation},
			&FilterableToRangeableStrategy{propNames: []string{prop}, generation: generation},
			&SearchableRetokenizeStrategy{propName: prop, generation: generation},
			&FilterableRetokenizeStrategy{propName: prop, generation: generation},
			&EnableFilterableStrategy{propNames: []string{prop}, generation: generation},
			&EnableSearchableStrategy{propNames: []string{prop}, generation: generation},
			&RebuildSearchableStrategy{propNames: []string{prop}, generation: generation},
		}
	}
	require.Len(t, strategiesFor("title", 1), len(strategiesByMigrationDir(1)))

	for _, generation := range []int{1, 2, 11} {
		for _, prop := range props {
			for _, strategy := range strategiesFor(prop, generation) {
				main := strategy.SourceBucketName(prop)
				requireAcceptedInPromoteRoles(t, main)
				for _, suffix := range []string{
					strategy.ReindexSuffix(), strategy.IngestSuffix(), strategy.BackupSuffix(),
				} {
					name := main + suffix
					require.Truef(t, migrationHandleIsSidecarShaped(name),
						"%T emits %q, and refusing it refuses the migration", strategy, name)
					requireAcceptedInPromoteRoles(t, name)
				}
			}
		}
	}
	for _, prop := range props {
		for _, indexType := range []string{"filterable", "searchable", "rangeable"} {
			main, ok := mainBucketForPropertyIndex(prop, indexType)
			require.True(t, ok, indexType)
			requireAcceptedInPromoteRoles(t, main)
		}
	}
}

func requireAcceptedInPromoteRoles(t *testing.T, handle string) {
	t.Helper()
	for _, role := range migrationShardRootDirectoryRoles(t) {
		if role.shape != migrationShapePropertyBucket {
			continue
		}
		env := migrationRecordEnvelope{
			Subject:    testMigrationSubject(1, StrategyCodeSearchableRetokenize, "title"),
			State:      MigrationStateIterating,
			Checkpoint: &MigrationCheckpoint{},
		}
		migrationDirectoryRolePlacers[role.field](&env, "title", handle)
		require.NoErrorf(t, validateMigrationHandles(env),
			"a writer puts %q in the %s role", handle, role.field)
	}
}

func TestEveryStrategyReadsTheMainBucketThisNames(t *testing.T) {
	const prop = "title"
	byCode := map[MigrationStrategyCode]MigrationStrategy{
		StrategyCodeSearchableMapToBlockmax:     &MapToBlockmaxStrategy{},
		StrategyCodeFilterableRoaringsetRefresh: &RoaringSetRefreshStrategy{},
		StrategyCodeFilterableToRangeable:       &FilterableToRangeableStrategy{propNames: []string{prop}},
		StrategyCodeSearchableRetokenize:        &SearchableRetokenizeStrategy{propName: prop},
		StrategyCodeFilterableRetokenize:        &FilterableRetokenizeStrategy{propName: prop},
		StrategyCodeEnableFilterable:            &EnableFilterableStrategy{propNames: []string{prop}},
		StrategyCodeEnableSearchable:            &EnableSearchableStrategy{propNames: []string{prop}},
		StrategyCodeRebuildSearchable:           &RebuildSearchableStrategy{propNames: []string{prop}},
	}
	require.Len(t, byCode, len(strategiesByMigrationDir(1)), "every strategy carries a record code")

	for code, strategy := range byCode {
		t.Run(string(code), func(t *testing.T) {
			require.True(t, code.valid())
			require.Equal(t, strategy.SourceBucketName(prop), sourceBucketNameFor(code, prop),
				"a record's canonical handle is derived from its code, so the two have to agree")
		})
	}
}

func TestACanonicalHandleMustBeTheRecordsOwnMainBucket(t *testing.T) {
	inRole := func(role, handle string) error {
		env := newMigrationRecordEnvelope(MigrationStateIterating,
			testMigrationSubject(1, StrategyCodeSearchableRetokenize, "title"))
		migrationDirectoryRolePlacers[role](&env, "title", handle)
		return validateMigrationEnvelope(env)
	}

	for _, tt := range []struct {
		name   string
		handle string
	}{
		{name: "the shard's own id index", handle: helpers.BucketFromPropNameLSM(filters.InternalPropID)},
		{name: "a property length index", handle: helpers.BucketFromPropNameLengthLSM("title")},
		{name: "another property's bucket", handle: helpers.BucketFromPropNameLSM("body")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, inRole(string(migrationRoleCanonical), tt.handle),
				"a canonical handle is the bucket this record's own property and strategy name")
			require.NoError(t, inRole("displaced directory", tt.handle),
				"a flip moves aside whatever was there, so any property bucket is a legal displaced handle")
		})
	}
}

// A record fsynced into a directory whose own name never reached disk reads
// after a crash as no record at all, which is the one outcome the freeze
// cannot see: the shard proceeds as if the migration had never run.
func TestTheRecordsDirectoryIsOnDiskBeforeARecordLandsInIt(t *testing.T) {
	logger, _ := test.NewNullLogger()
	lsmPath := t.TempDir()
	store := NewMigrationRecordStore(lsmPath, logger)

	var synced []string
	record := func(path string) error {
		synced = append(synced, path)
		return nil
	}

	require.NoError(t, store.mkdirSynced(record))
	require.Equal(t, []string{lsmPath, filepath.Join(lsmPath, migrationsDir)}, synced,
		"each directory this call created has to have its own name published by its parent")

	synced = nil
	require.NoError(t, store.mkdirSynced(record))
	require.Empty(t, synced, "a directory that was already there was not created, so nothing was published")

	fresh := NewMigrationRecordStore(t.TempDir(), logger)
	require.Equal(t, assert.AnError, fresh.mkdirSynced(func(string) error { return assert.AnError }),
		"a directory whose name did not reach disk must not read as a store ready to be written to")
}

// The two counters were persisted but never written or read. Retiring them
// only holds if a record an older build wrote still loads: a record this build
// cannot decode withholds every destructive action on its shard.
func TestARecordCarryingTheRetiredCheckpointCountersStillLoads(t *testing.T) {
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	data, err := json.Marshal(migrationRecordEnvelope{
		// A literal, not the constant: an older build stamped the version it
		// knew, so bumping the constant has to red this test rather than move
		// the fixture along with it.
		FormatVersion: 1,
		State:         MigrationStateIterating,
		Subject:       subject,
		Checkpoint:    &MigrationCheckpoint{LastProcessedKey: []byte("halfway")},
	})
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	checkpoint := raw["checkpoint"].(map[string]any)
	checkpoint["processedCount"], checkpoint["indexedCount"] = 1200, 980
	asOlderBuildsWroteIt, err := json.Marshal(raw)
	require.NoError(t, err)

	rec, err := decodeMigrationRecord(asOlderBuildsWroteIt)
	require.NoError(t, err, "a record an older build wrote must not read as one this build cannot place")
	require.Equal(t, MigrationStateIterating, rec.State())
	require.Equal(t, []byte("halfway"), rec.(MigrationRecordIterating).Checkpoint().LastProcessedKey,
		"and the one field still read survives the two that are gone")
}

// A failed Put must adopt exactly the record the file holds after a
// rename-then-sync race, not stale in-memory state.
func TestAFailedPutAdoptsOnlyTheRecordTheFileHolds(t *testing.T) {
	tests := []struct {
		name        string
		onDisk      func(MigrationSubject) MigrationRecord
		wantAdopted bool
	}{
		{
			name: "the rename ran, so the file already holds what the write was publishing",
			onDisk: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties(), map[string]string{"title": "property_title"})
			},
			wantAdopted: true,
		},
		{
			name:   "the rename never ran, so the file still holds the older record",
			onDisk: func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			require.NoError(t, os.MkdirAll(store.Dir(), 0o777))

			subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
			planted, err := encodeMigrationRecord(tt.onDisk(subject))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(store.Dir(), subject.Key.fileName()), planted, 0o600))

			denyDirectoryWrites(t, store.Dir())
			writing := NewMigrationRecordSwapped(subject, subject.Properties(),
				map[string]string{"title": "property_title"})
			require.Error(t, store.Put(writing), "the write could not finish, so it has to report a failure")

			got, present := store.Get(subject.Key)
			require.Equal(t, tt.wantAdopted, present,
				"the store has to answer with what the file holds, not with what the write intended")
			if tt.wantAdopted {
				require.Equal(t, MigrationStateSwapped, got.State())
			}

			kept, err := os.ReadFile(filepath.Join(store.Dir(), subject.Key.fileName()))
			require.NoError(t, err)
			require.Equal(t, planted, kept, "and a publish that failed leaves the file it found")
		})
	}
}

// Put syncs the directory that publishes a record's name. Without the same
// sync on the unlink, a crash brings a retired record back, naming directories
// the pass that retired it already removed.
func TestRemovingARecordPublishesTheRemoval(t *testing.T) {
	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	key := testMigrationSubject(42, StrategyCodeEnableFilterable, "title").Key
	put := func() {
		require.NoError(t, store.Put(NewMigrationRecordMerged(
			testMigrationSubject(42, StrategyCodeEnableFilterable, "title"))))
	}

	put()
	var synced []string
	require.NoError(t, store.removeSynced(key, func(dir string) error {
		require.NoFileExists(t, filepath.Join(store.Dir(), key.fileName()),
			"the sync has to follow the unlink it makes durable")
		synced = append(synced, dir)
		return nil
	}))
	require.Equal(t, []string{store.Dir()}, synced,
		"the directory that held the record's name is what publishes its absence")

	synced = nil
	require.NoError(t, store.removeSynced(key, func(dir string) error {
		synced = append(synced, dir)
		return nil
	}))
	require.Empty(t, synced, "a removal that found no file has nothing to publish")

	put()
	require.Error(t, store.removeSynced(key, func(string) error { return assert.AnError }),
		"a removal whose absence did not reach disk must not read as done")
	require.Empty(t, store.Records(), "the file is gone either way, so memory has to agree")
}

// The wedge belongs to the record the key named, not to the key. A record
// re-created under a removed key would inherit it and stay hidden from the
// periodic pass for the life of the process.
func TestRemovingARecordTakesItsWedgeWithIt(t *testing.T) {
	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")

	require.NoError(t, store.Put(NewMigrationRecordMerged(subject)))
	store.MarkWedged(subject.Key)
	require.False(t, store.HasUndecided(), "fixture: a wedged record stops driving the pass")

	require.NoError(t, store.Remove(subject.Key))
	require.NoError(t, store.Put(NewMigrationRecordMerged(subject)))

	require.False(t, store.Wedged(subject.Key))
	require.True(t, store.HasUndecided(),
		"the re-created record has never been decided, so the pass has to come back for it")
}

// A restore or a replica move lands another replica's record file under this
// shard. Adopted, it is granted a seal no local worker holds and is reconciled
// as local work, and this shard's own record then puts two units in one
// directory, which freezes every write until someone deletes a file by hand.
func TestALoneForeignRecordIsSetAsideRatherThanAdopted(t *testing.T) {
	logger, _ := test.NewNullLogger()
	lsmPath := t.TempDir()
	const own = "shard-1__node-0"

	foreign := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
	foreign.Key.UnitID = "shard-1__node-9"
	require.NoError(t, NewMigrationRecordStore(lsmPath, logger).Put(NewMigrationRecordMerged(foreign)))

	local := NewMigrationRecordStoreForUnit(lsmPath, own, logger)
	require.NoError(t, local.Load())
	require.Empty(t, local.Records(), "another replica's record is not this shard's work to reconcile")
	require.Empty(t, local.Unreadable(), "and it is not a fault, so nothing on this shard is withheld")

	mine := testMigrationSubject(43, StrategyCodeEnableFilterable, "title")
	require.Equal(t, own, mine.Key.UnitID, "fixture: the second record is this shard's own")
	require.NoError(t, local.Put(NewMigrationRecordMerged(mine)))

	require.NoError(t, local.Load())
	require.Len(t, local.Records(), 1, "only this shard's own record")
	require.Empty(t, local.Unreadable(),
		"a shard that knows its own unit must not freeze over a file another replica left")

	_, err := os.Stat(filepath.Join(local.Dir(), foreign.Key.fileName()))
	require.NoError(t, err, "the foreign file is left on disk for whoever put it there")
}
