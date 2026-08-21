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
	"encoding/json"
	"fmt"
	"time"
)

// MigrationState is the durable state of one shard-local reindex migration.
// Absent — no record on disk — is the null, and has no constant.
type MigrationState string

const (
	MigrationStateIterating MigrationState = "iterating"
	MigrationStateIterated  MigrationState = "iterated"
	MigrationStateMerged    MigrationState = "merged"
	MigrationStateSwapped   MigrationState = "swapped"
	MigrationStatePromoted  MigrationState = "promoted"
)

// MigrationStrategyCode names the strategy a record belongs to. The values
// land in record file names, so they are a durable on-disk format: never
// rename one, and never reuse a retired one.
type MigrationStrategyCode string

const (
	StrategyCodeSearchableMapToBlockmax     MigrationStrategyCode = "searchable_map_to_blockmax"
	StrategyCodeFilterableRoaringsetRefresh MigrationStrategyCode = "filterable_roaringset_refresh"
	StrategyCodeFilterableToRangeable       MigrationStrategyCode = "filterable_to_rangeable"
	StrategyCodeSearchableRetokenize        MigrationStrategyCode = "searchable_retokenize"
	StrategyCodeFilterableRetokenize        MigrationStrategyCode = "filterable_retokenize"
	StrategyCodeEnableFilterable            MigrationStrategyCode = "enable_filterable"
	StrategyCodeEnableSearchable            MigrationStrategyCode = "enable_searchable"
	StrategyCodeRebuildSearchable           MigrationStrategyCode = "rebuild_searchable"
)

func (c MigrationStrategyCode) valid() bool {
	switch c {
	case StrategyCodeSearchableMapToBlockmax, StrategyCodeFilterableRoaringsetRefresh,
		StrategyCodeFilterableToRangeable, StrategyCodeSearchableRetokenize,
		StrategyCodeFilterableRetokenize, StrategyCodeEnableFilterable,
		StrategyCodeEnableSearchable, StrategyCodeRebuildSearchable:
		return true
	default:
		return false
	}
}

func migrationTypeKnown(t ReindexMigrationType) bool {
	switch t {
	case ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable, ReindexTypeEnableRangeable,
		ReindexTypeRepairRangeable, ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable, ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable:
		return true
	default:
		return false
	}
}

// MigrationRecordKey identifies one migration on one shard. TaskVersion is the
// RAFT log index of the task's creation, so it is also the generation: a total
// order allocated by consensus, identical on every node, which is what lets
// two records on one property be compared without chasing links.
type MigrationRecordKey struct {
	TaskVersion  uint64                `json:"taskVersion"`
	StrategyCode MigrationStrategyCode `json:"strategyCode"`
	UnitID       string                `json:"unitID"`
}

// fileName omits the unit because a unit is "<shard>__<node>": every record
// under one shard directory carries the same one, so the pair left is unique
// there. A change-tokenization payload fans into two strategies, which is why
// the code has to be in the name at all.
func (k MigrationRecordKey) fileName() string {
	return fmt.Sprintf("%d_%s.json", k.TaskVersion, k.StrategyCode)
}

func (k MigrationRecordKey) String() string {
	return fmt.Sprintf("%d/%s/%s", k.TaskVersion, k.StrategyCode, k.UnitID)
}

func (k MigrationRecordKey) valid() bool {
	return k.TaskVersion > 0 && k.StrategyCode.valid() && k.UnitID != ""
}

// MigrationCheckpoint is the iteration resume point.
type MigrationCheckpoint struct {
	LastProcessedKey []byte    `json:"lastProcessedKey,omitempty"`
	ProcessedCount   int       `json:"processedCount"`
	IndexedCount     int       `json:"indexedCount"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

// MigrationSubject is what every variant carries: enough to identify the
// migration and to name every directory it touches, so that no reader ever
// re-derives a directory from a property name or a generation number.
type MigrationSubject struct {
	Key                  MigrationRecordKey   `json:"key"`
	TaskID               string               `json:"taskID"`
	MigrationType        ReindexMigrationType `json:"migrationType"`
	Properties           []string             `json:"properties,omitempty"`
	TargetTokenization   string               `json:"targetTokenization,omitempty"`
	OriginalTokenization string               `json:"originalTokenization,omitempty"`

	// IterationCutoff is the horizon the rebuild iterates up to: an object
	// last updated at or after it is left to the double-write mirror. It is
	// captured in the same act as this record's first write, which is what
	// keeps the window between arming the mirror and fixing the horizon
	// empty. Every later state carries it unchanged, so a resume never
	// re-derives a horizon from a clock that has moved on.
	IterationCutoff time.Time `json:"iterationCutoff"`

	// StagedDirs is the re-derivation's "live-data dir": per property, the
	// directory holding this migration's own data. The flip makes it live and
	// promotion renames it onto CanonicalDirs.
	StagedDirs    map[string]string `json:"stagedDirs,omitempty"`
	CanonicalDirs map[string]string `json:"canonicalDirs,omitempty"`
	SidecarDirs   []string          `json:"sidecarDirs,omitempty"`

	// DisplacementLinks names, per property, every record extant when this one
	// was first written whose property set overlapped. Bookkeeping for the
	// retirement arithmetic only: supersession is ordered by TaskVersion, so
	// nothing here decides which of two records is newer.
	DisplacementLinks map[string][]MigrationRecordKey `json:"displacementLinks,omitempty"`
}

// MigrationRecord is the sealed set of five variants. Only this package can
// implement it, and a record is a value: whoever holds one must not mutate it
// or anything reachable from it, because the store hands the same value to
// every concurrent reader.
type MigrationRecord interface {
	State() MigrationState
	Subject() MigrationSubject
	migrationRecordQuestions

	// toEnvelope both serializes and seals: an unexported method keeps the
	// variant set closed to this package.
	toEnvelope() migrationRecordEnvelope
}

type migrationRecordBase struct {
	subject MigrationSubject
}

func (b migrationRecordBase) Subject() MigrationSubject { return b.subject }

// migrationFlipBlock is the flip decision: which properties the record's flip
// covers, and per property the directory that flip displaced. Both are
// resolvable only at the moment of the flip, which is why they are written
// then rather than re-derived later.
type migrationFlipBlock struct {
	flipped       []string
	displacedDirs map[string]string
}

func (f migrationFlipBlock) Flipped() []string { return f.flipped }

func (f migrationFlipBlock) DisplacedDir(prop string) (string, bool) {
	dir, ok := f.displacedDirs[prop]
	return dir, ok
}

type MigrationRecordIterating struct {
	migrationRecordBase
	checkpoint MigrationCheckpoint
}

type MigrationRecordIterated struct {
	migrationRecordBase
}

type MigrationRecordMerged struct {
	migrationRecordBase
}

type MigrationRecordSwapped struct {
	migrationRecordBase
	migrationFlipBlock

	// runtimeFlipped is the in-process flip window and is never serialized:
	// nil means every recorded flip is done, which is what a load always sees.
	runtimeFlipped map[string]struct{}
}

type MigrationRecordPromoted struct {
	migrationRecordBase
	migrationFlipBlock
}

func NewMigrationRecordIterating(subject MigrationSubject, checkpoint MigrationCheckpoint) MigrationRecordIterating {
	return MigrationRecordIterating{migrationRecordBase{subject}, checkpoint}
}

func NewMigrationRecordIterated(subject MigrationSubject) MigrationRecordIterated {
	return MigrationRecordIterated{migrationRecordBase{subject}}
}

func NewMigrationRecordMerged(subject MigrationSubject) MigrationRecordMerged {
	return MigrationRecordMerged{migrationRecordBase{subject}}
}

func NewMigrationRecordSwapped(subject MigrationSubject, flipped []string, displacedDirs map[string]string) MigrationRecordSwapped {
	return MigrationRecordSwapped{migrationRecordBase: migrationRecordBase{subject}, migrationFlipBlock: migrationFlipBlock{flipped, displacedDirs}}
}

func NewMigrationRecordPromoted(subject MigrationSubject, flipped []string, displacedDirs map[string]string) MigrationRecordPromoted {
	return MigrationRecordPromoted{migrationRecordBase{subject}, migrationFlipBlock{flipped, displacedDirs}}
}

func (r MigrationRecordIterating) Checkpoint() MigrationCheckpoint { return r.checkpoint }

func (r MigrationRecordIterating) State() MigrationState { return MigrationStateIterating }
func (r MigrationRecordIterated) State() MigrationState  { return MigrationStateIterated }
func (r MigrationRecordMerged) State() MigrationState    { return MigrationStateMerged }
func (r MigrationRecordSwapped) State() MigrationState   { return MigrationStateSwapped }
func (r MigrationRecordPromoted) State() MigrationState  { return MigrationStatePromoted }

// migrationRecordFormatVersion is bumped only for a change a previous release
// cannot read correctly. An older node treats an unknown version as
// NotUnderstood, which withholds destructive work rather than guessing.
const migrationRecordFormatVersion = 1

type migrationFlipEnvelope struct {
	Flipped       []string          `json:"flipped,omitempty"`
	DisplacedDirs map[string]string `json:"displacedDirs,omitempty"`
}

type migrationRecordEnvelope struct {
	FormatVersion int                    `json:"formatVersion"`
	State         MigrationState         `json:"state"`
	Subject       MigrationSubject       `json:"subject"`
	Checkpoint    *MigrationCheckpoint   `json:"checkpoint,omitempty"`
	Flip          *migrationFlipEnvelope `json:"flip,omitempty"`
}

func newMigrationRecordEnvelope(state MigrationState, subject MigrationSubject) migrationRecordEnvelope {
	return migrationRecordEnvelope{FormatVersion: migrationRecordFormatVersion, State: state, Subject: subject}
}

func (f migrationFlipBlock) toEnvelope() *migrationFlipEnvelope {
	return &migrationFlipEnvelope{Flipped: f.flipped, DisplacedDirs: f.displacedDirs}
}

func (r MigrationRecordIterating) toEnvelope() migrationRecordEnvelope {
	env := newMigrationRecordEnvelope(MigrationStateIterating, r.subject)
	cp := r.checkpoint
	env.Checkpoint = &cp
	return env
}

func (r MigrationRecordIterated) toEnvelope() migrationRecordEnvelope {
	return newMigrationRecordEnvelope(MigrationStateIterated, r.subject)
}

func (r MigrationRecordMerged) toEnvelope() migrationRecordEnvelope {
	return newMigrationRecordEnvelope(MigrationStateMerged, r.subject)
}

func (r MigrationRecordSwapped) toEnvelope() migrationRecordEnvelope {
	env := newMigrationRecordEnvelope(MigrationStateSwapped, r.subject)
	env.Flip = r.migrationFlipBlock.toEnvelope()
	return env
}

func (r MigrationRecordPromoted) toEnvelope() migrationRecordEnvelope {
	env := newMigrationRecordEnvelope(MigrationStatePromoted, r.subject)
	env.Flip = r.migrationFlipBlock.toEnvelope()
	return env
}

// encodeMigrationRecord indents so an operator can read a record with cat;
// records are written once per transition, never on a hot path.
func encodeMigrationRecord(rec MigrationRecord) ([]byte, error) {
	return json.MarshalIndent(rec.toEnvelope(), "", "  ")
}

// decodeMigrationRecord rejects anything it cannot place exactly, including a
// state-specific block on a state that has none. Every rejection becomes
// NotUnderstood, which preserves rather than deletes.
func decodeMigrationRecord(data []byte) (MigrationRecord, error) {
	var env migrationRecordEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	if env.FormatVersion != migrationRecordFormatVersion {
		return nil, fmt.Errorf("unknown record format version %d", env.FormatVersion)
	}
	if !env.Subject.Key.valid() {
		return nil, fmt.Errorf("record key %q is incomplete or names an unknown strategy", env.Subject.Key)
	}
	if env.Subject.TaskID == "" {
		return nil, fmt.Errorf("record %q has no task ID", env.Subject.Key)
	}
	if !migrationTypeKnown(env.Subject.MigrationType) {
		return nil, fmt.Errorf("record %q names unknown migration type %q", env.Subject.Key, env.Subject.MigrationType)
	}

	switch env.State {
	case MigrationStateIterating:
		if err := env.requireBlocks(true, false); err != nil {
			return nil, err
		}
		return NewMigrationRecordIterating(env.Subject, *env.Checkpoint), nil
	case MigrationStateIterated:
		if err := env.requireBlocks(false, false); err != nil {
			return nil, err
		}
		return NewMigrationRecordIterated(env.Subject), nil
	case MigrationStateMerged:
		if err := env.requireBlocks(false, false); err != nil {
			return nil, err
		}
		return NewMigrationRecordMerged(env.Subject), nil
	case MigrationStateSwapped:
		if err := env.requireBlocks(false, true); err != nil {
			return nil, err
		}
		return NewMigrationRecordSwapped(env.Subject, env.Flip.Flipped, env.Flip.DisplacedDirs), nil
	case MigrationStatePromoted:
		if err := env.requireBlocks(false, true); err != nil {
			return nil, err
		}
		return NewMigrationRecordPromoted(env.Subject, env.Flip.Flipped, env.Flip.DisplacedDirs), nil
	default:
		return nil, fmt.Errorf("record %q names unknown state %q", env.Subject.Key, env.State)
	}
}

func (e migrationRecordEnvelope) requireBlocks(checkpoint, flip bool) error {
	if (e.Checkpoint != nil) != checkpoint {
		return fmt.Errorf("record %q in state %q: checkpoint block present=%v, wanted=%v",
			e.Subject.Key, e.State, e.Checkpoint != nil, checkpoint)
	}
	if (e.Flip != nil) != flip {
		return fmt.Errorf("record %q in state %q: flip block present=%v, wanted=%v",
			e.Subject.Key, e.State, e.Flip != nil, flip)
	}
	return nil
}
