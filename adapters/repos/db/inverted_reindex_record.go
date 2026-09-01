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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

type MigrationState string

const (
	MigrationStateIterating MigrationState = "iterating"
	MigrationStateIterated  MigrationState = "iterated"
	MigrationStateMerged    MigrationState = "merged"
	MigrationStateSwapped   MigrationState = "swapped"
	MigrationStatePromoted  MigrationState = "promoted"
)

// Values land in record file names, so they're a durable on-disk format:
// never rename one, and never reuse a retired one.
type MigrationStrategyCode string

const (
	StrategyCodeSearchableMapToBlockmax     MigrationStrategyCode = MigrationDirSearchableMapToBlockmax
	StrategyCodeFilterableRoaringsetRefresh MigrationStrategyCode = MigrationDirFilterableRoaringsetRefresh
	StrategyCodeFilterableToRangeable       MigrationStrategyCode = MigrationDirPrefixFilterableToRangeable
	StrategyCodeSearchableRetokenize        MigrationStrategyCode = MigrationDirPrefixSearchableRetokenize
	StrategyCodeFilterableRetokenize        MigrationStrategyCode = MigrationDirPrefixFilterableRetokenize
	StrategyCodeEnableFilterable            MigrationStrategyCode = MigrationDirPrefixEnableFilterable
	StrategyCodeEnableSearchable            MigrationStrategyCode = MigrationDirPrefixEnableSearchable
	StrategyCodeRebuildSearchable           MigrationStrategyCode = MigrationDirPrefixRebuildSearchable
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

// TaskVersion is not the generation: that's a separate per-node counter
// nodes routinely disagree on.
type MigrationRecordKey struct {
	TaskVersion  uint64                `json:"taskVersion"`
	StrategyCode MigrationStrategyCode `json:"strategyCode"`
	UnitID       string                `json:"unitID"`
}

// MigrationUnitID composes the ID of the reindex task unit that covers one
// shard replica. The submit path names units with it, and a shard derives its
// own with it, so a record found under a shard can be told apart from one a
// backup or a shard copy landed there from another replica.
func MigrationUnitID(shardName, nodeName string) string {
	return shardName + "__" + nodeName
}

// The unit is in the name because backup and shard copy land a foreign node's
// record here; without it the names collide and the next local write destroys it.
func (k MigrationRecordKey) fileName() string {
	return fmt.Sprintf("%d_%s_%s.json", k.TaskVersion, k.StrategyCode, k.UnitID)
}

func (k MigrationRecordKey) String() string {
	return fmt.Sprintf("%d/%s/%s", k.TaskVersion, k.StrategyCode, k.UnitID)
}

func (k MigrationRecordKey) valid() bool {
	return k.TaskVersion > 0 && k.StrategyCode.valid() && migrationHandleIsOneElement(k.UnitID)
}

// MigrationCheckpoint is the iteration resume point.
type MigrationCheckpoint struct {
	LastProcessedKey []byte    `json:"lastProcessedKey,omitempty"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type MigrationPropertyDirs struct {
	Staged    string `json:"staged,omitempty"`
	Canonical string `json:"canonical,omitempty"`
	Sidecar   string `json:"sidecar,omitempty"`
}

// Carries enough to name every directory it touches, so no reader ever
// re-derives one from a property name or a generation number. Naming a
// directory is what lists its property, so a directory for a property the
// record does not cover is unwritable.
type MigrationSubject struct {
	Key                  MigrationRecordKey   `json:"key"`
	TaskID               string               `json:"taskID"`
	MigrationType        ReindexMigrationType `json:"migrationType"`
	TargetTokenization   string               `json:"targetTokenization,omitempty"`
	OriginalTokenization string               `json:"originalTokenization,omitempty"`

	// Fixed at first write, never re-derived from a moved clock.
	IterationCutoff time.Time `json:"iterationCutoff"`

	TrackerDir string `json:"trackerDir,omitempty"`

	// Unmirrored is set when a boot could not arm this migration's double-write
	// mirror. Writes taken in that window reach the canonical bucket only, so
	// the staged copy is permanently behind and must never rename over it.
	Unmirrored bool `json:"unmirrored,omitempty"`

	Props map[string]MigrationPropertyDirs `json:"props,omitempty"`
}

// Sorted, so every pass over a record's properties reaches them in one order.
func (s MigrationSubject) Properties() []string { return slices.Sorted(maps.Keys(s.Props)) }

func migrationStagedOf(d MigrationPropertyDirs) string    { return d.Staged }
func migrationCanonicalOf(d MigrationPropertyDirs) string { return d.Canonical }
func migrationSidecarOf(d MigrationPropertyDirs) string   { return d.Sidecar }

func (s MigrationSubject) dirsInRole(read func(MigrationPropertyDirs) string) map[string]string {
	out := make(map[string]string, len(s.Props))
	for prop, dirs := range s.Props {
		out[prop] = read(dirs)
	}
	return out
}

var migrationHorizonEverything = time.Date(9999, time.January, 1, 0, 0, 0, 0, time.UTC)

// A record is a value: whoever holds one must not mutate it or anything
// reachable from it, since the store hands the same value to every reader.
type MigrationRecord interface {
	State() MigrationState
	Subject() MigrationSubject
	migrationRecordQuestions

	toEnvelope() migrationRecordEnvelope
}

type migrationRecordBase struct {
	subject MigrationSubject
}

func (b migrationRecordBase) Subject() MigrationSubject { return b.subject }

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
	promotion map[string]migrationPromotionMark
}

type migrationPromotionMark string

const (
	migrationPromotionStarted  migrationPromotionMark = "started"
	migrationPromotionFinished migrationPromotionMark = "finished"
	migrationPromotionLost     migrationPromotionMark = "lost"
)

func migrationPromotionMarkKnown(m migrationPromotionMark) bool {
	switch m {
	case migrationPromotionStarted, migrationPromotionFinished, migrationPromotionLost:
		return true
	}
	return false
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
	return MigrationRecordSwapped{migrationRecordBase{subject}, migrationFlipBlock{flipped, displacedDirs}, nil}
}

func (r MigrationRecordSwapped) PromotionOf(prop string) migrationPromotionMark {
	return r.promotion[prop]
}

func (r MigrationRecordSwapped) WithPromotionAt(prop string, mark migrationPromotionMark) MigrationRecordSwapped {
	next := make(map[string]migrationPromotionMark, len(r.promotion)+1)
	maps.Copy(next, r.promotion)
	next[prop] = mark
	r.promotion = next
	return r
}

func (r MigrationRecordSwapped) WithPromotionAbandoned(prop string) MigrationRecordSwapped {
	if _, marked := r.promotion[prop]; !marked {
		return r
	}
	next := make(map[string]migrationPromotionMark, len(r.promotion))
	maps.Copy(next, r.promotion)
	delete(next, prop)
	r.promotion = next
	return r
}

func NewMigrationRecordPromoted(subject MigrationSubject, flipped []string, displacedDirs map[string]string) MigrationRecordPromoted {
	return MigrationRecordPromoted{migrationRecordBase{subject}, migrationFlipBlock{flipped, displacedDirs}}
}

// Read by the cutover PR, which resumes an interrupted rebuild from it.
func (r MigrationRecordIterating) Checkpoint() MigrationCheckpoint { return r.checkpoint }

func (r MigrationRecordIterating) State() MigrationState { return MigrationStateIterating }
func (r MigrationRecordIterated) State() MigrationState  { return MigrationStateIterated }
func (r MigrationRecordMerged) State() MigrationState    { return MigrationStateMerged }
func (r MigrationRecordSwapped) State() MigrationState   { return MigrationStateSwapped }
func (r MigrationRecordPromoted) State() MigrationState  { return MigrationStatePromoted }

// Bumped only for changes a previous release can't read: a bump freezes
// every record already on the shard as NotUnderstood on the other build.
const migrationRecordFormatVersion = 1

type migrationFlipEnvelope struct {
	Flipped       []string          `json:"flipped,omitempty"`
	DisplacedDirs map[string]string `json:"displacedDirs,omitempty"`
	// Its own key, not one an earlier shape wrote: a build that reads only the
	// other key then promotes nothing rather than promoting on a claim it cannot check.
	Promotion map[string]migrationPromotionMark `json:"promotion,omitempty"`
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
	env.Flip.Promotion = r.promotion
	return env
}

func (r MigrationRecordPromoted) toEnvelope() migrationRecordEnvelope {
	env := newMigrationRecordEnvelope(MigrationStatePromoted, r.subject)
	env.Flip = r.migrationFlipBlock.toEnvelope()
	return env
}

func encodeMigrationRecord(rec MigrationRecord) ([]byte, error) {
	env := rec.toEnvelope()
	if err := validateMigrationEnvelope(env); err != nil {
		return nil, err
	}
	// Writer-side only: nothing acts on such a record, so refusing it at decode
	// would freeze a shard over a record that can do nothing.
	if len(env.Subject.Props) == 0 {
		return nil, fmt.Errorf("record %q names no properties, so nothing could ever act on it", env.Subject.Key)
	}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(data) > maxMigrationRecordBytes {
		return nil, fmt.Errorf("record %q holds %d bytes, bound is %d",
			env.Subject.Key, len(data), maxMigrationRecordBytes)
	}
	return data, nil
}

func validateMigrationEnvelope(e migrationRecordEnvelope) error {
	if e.FormatVersion != migrationRecordFormatVersion {
		return fmt.Errorf("unknown record format version %d", e.FormatVersion)
	}
	if !e.Subject.Key.valid() {
		return fmt.Errorf("record key %q is incomplete or names an unknown strategy", e.Subject.Key)
	}
	if e.Subject.TaskID == "" {
		return fmt.Errorf("record %q has no task ID", e.Subject.Key)
	}
	if !migrationTypeKnown(e.Subject.MigrationType) {
		return fmt.Errorf("record %q names unknown migration type %q", e.Subject.Key, e.Subject.MigrationType)
	}
	if err := validateMigrationHandles(e); err != nil {
		return err
	}
	if err := validatePromotion(e); err != nil {
		return err
	}
	if err := validateFlipCoversEveryProperty(e); err != nil {
		return err
	}
	if err := validateOneOwnerPerDirectory(e); err != nil {
		return err
	}
	return validateCanonicalNamesOwnBucket(e)
}

// Promotion renames over every property the record names, and before the flip
// a property's canonical bucket is its complete primary copy. A flip that
// covers fewer properties than the record names would take one of those, and
// one that displaces a directory for a property outside the list claims a
// directory no reader here would ever reclaim or hand back.
func validateFlipCoversEveryProperty(e migrationRecordEnvelope) error {
	if e.Flip == nil {
		return nil
	}
	flipped := make(map[string]struct{}, len(e.Flip.Flipped))
	for _, prop := range e.Flip.Flipped {
		flipped[prop] = struct{}{}
	}
	for _, prop := range e.Subject.Properties() {
		if _, covered := flipped[prop]; !covered {
			return fmt.Errorf("record %q names property %q, which its flip does not cover", e.Subject.Key, prop)
		}
		delete(flipped, prop)
	}
	if len(flipped) > 0 {
		return fmt.Errorf("record %q flips %d property/properties it does not name", e.Subject.Key, len(flipped))
	}
	for _, prop := range slices.Sorted(maps.Keys(e.Flip.DisplacedDirs)) {
		if _, named := e.Subject.Props[prop]; !named {
			return fmt.Errorf("record %q displaces a directory for property %q, which it does not name",
				e.Subject.Key, prop)
		}
	}
	return nil
}

// Not a shape test: the promotion renames the staged directory over this one,
// so a canonical handle naming any other bucket renames over data no migration
// here built. Empty stays legal; the record then promotes nothing.
func validateCanonicalNamesOwnBucket(e migrationRecordEnvelope) error {
	for _, prop := range e.Subject.Properties() {
		canonical := e.Subject.Props[prop].Canonical
		want := sourceBucketNameFor(e.Subject.Key.StrategyCode, prop)
		if canonical != "" && canonical != want {
			return fmt.Errorf("record %q names %q as the canonical directory of property %q, but its strategy reads %q",
				e.Subject.Key, canonical, prop, want)
		}
	}
	return nil
}

// [TestEveryStrategyReadsTheMainBucketThisNames] pins this against the eight
// strategies' own SourceBucketName. No default arm, so the linter refuses a
// ninth code that names no bucket; a valid key never reaches the last return.
func sourceBucketNameFor(code MigrationStrategyCode, prop string) string {
	switch code {
	case StrategyCodeSearchableMapToBlockmax, StrategyCodeEnableSearchable,
		StrategyCodeRebuildSearchable, StrategyCodeSearchableRetokenize:
		return helpers.BucketSearchableFromPropNameLSM(prop)
	case StrategyCodeFilterableToRangeable:
		return helpers.BucketRangeableFromPropNameLSM(prop)
	case StrategyCodeFilterableRoaringsetRefresh, StrategyCodeFilterableRetokenize,
		StrategyCodeEnableFilterable:
		return helpers.BucketFromPropNameLSM(prop)
	}
	return ""
}

func validatePromotion(e migrationRecordEnvelope) error {
	if e.Flip == nil {
		return nil
	}
	if len(e.Flip.Promotion) > 0 && e.State != MigrationStateSwapped {
		return fmt.Errorf("record %q in state %q carries promotion marks, which only a swapped record does",
			e.Subject.Key, e.State)
	}
	for _, prop := range slices.Sorted(maps.Keys(e.Flip.Promotion)) {
		if _, named := e.Subject.Props[prop]; !named {
			return fmt.Errorf("record %q records a promotion of property %q, which it does not name",
				e.Subject.Key, prop)
		}
		if !migrationPromotionMarkKnown(e.Flip.Promotion[prop]) {
			return fmt.Errorf("record %q records unknown promotion mark %q for property %q",
				e.Subject.Key, e.Flip.Promotion[prop], prop)
		}
	}
	return nil
}

// Each actor acts on one property's handles, so a directory a second property
// also names gets closed or deleted while that property is still serving from it.
func validateOneOwnerPerDirectory(e migrationRecordEnvelope) error {
	type claim struct{ role, prop string }
	owner := map[string]claim{}

	for _, group := range migrationHandleGroups {
		if group.dirs == nil {
			continue
		}
		dirs := group.dirs(e)
		for _, prop := range slices.Sorted(maps.Keys(dirs)) {
			dir := dirs[prop]
			if dir == "" || (group.displacesCanonical && dir == e.Subject.Props[prop].Canonical) {
				continue
			}
			if held, taken := owner[dir]; taken {
				return fmt.Errorf(
					"record %q names directory %q as both the %s of property %q and the %s of property %q",
					e.Subject.Key, dir, held.role, held.prop, group.field, prop)
			}
			owner[dir] = claim{group.field, prop}
		}
	}
	return nil
}

func decodeMigrationRecord(data []byte) (MigrationRecord, error) {
	var env migrationRecordEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	if err := validateMigrationEnvelope(env); err != nil {
		return nil, err
	}

	switch env.State {
	case MigrationStateIterating:
		if err := env.requireBlocks(migrationBlocks{checkpoint: true}); err != nil {
			return nil, err
		}
		return NewMigrationRecordIterating(env.Subject, *env.Checkpoint), nil
	case MigrationStateIterated:
		if err := env.requireBlocks(migrationBlocks{}); err != nil {
			return nil, err
		}
		return NewMigrationRecordIterated(env.Subject), nil
	case MigrationStateMerged:
		if err := env.requireBlocks(migrationBlocks{}); err != nil {
			return nil, err
		}
		return NewMigrationRecordMerged(env.Subject), nil
	case MigrationStateSwapped:
		if err := env.requireBlocks(migrationBlocks{flip: true}); err != nil {
			return nil, err
		}
		swapped := NewMigrationRecordSwapped(env.Subject, env.Flip.Flipped, env.Flip.DisplacedDirs)
		swapped.promotion = env.Flip.Promotion
		return swapped, nil
	case MigrationStatePromoted:
		if err := env.requireBlocks(migrationBlocks{flip: true}); err != nil {
			return nil, err
		}
		return NewMigrationRecordPromoted(env.Subject, env.Flip.Flipped, env.Flip.DisplacedDirs), nil
	default:
		return nil, fmt.Errorf("record %q names unknown state %q", env.Subject.Key, env.State)
	}
}

// [filepath.IsLocal] isn't this test: it accepts ".", "x/.." and
// "a/b/../..", each of which Join resolves back to the shard root.
func migrationHandleIsOneElement(h string) bool {
	if h == "" || h == "." || h == ".." || filepath.IsAbs(h) {
		return false
	}
	return !strings.ContainsRune(h, '/') && !strings.ContainsRune(h, os.PathSeparator)
}

// [TestEveryDirectoryRoleUnderTheShardRootCarriesAShapeRule] refuses a role
// whose shape is neither the sidecar nor the property-bucket rule.
type migrationHandleShape uint8

const (
	migrationShapeUnchecked migrationHandleShape = iota
	migrationShapeSidecar
	migrationShapePropertyBucket
)

type migrationHandleGroup struct {
	field string

	dirs func(e migrationRecordEnvelope) map[string]string

	envelopeHandles func(e migrationRecordEnvelope) []string

	displacesCanonical bool
	// namesDirectory separates handles a sweep hands to os.RemoveAll from
	// property names, which are user-chosen and not checked against the
	// reserved set.
	namesDirectory bool

	underMigrationsDir bool

	shape migrationHandleShape
}

func (g migrationHandleGroup) handles(e migrationRecordEnvelope) []string {
	if g.dirs == nil {
		return g.envelopeHandles(e)
	}
	dirs := g.dirs(e)
	out := make([]string, 0, len(dirs))
	for _, prop := range slices.Sorted(maps.Keys(dirs)) {
		out = append(out, dirs[prop])
	}
	return out
}

var migrationHandleGroups = []migrationHandleGroup{
	{
		field:          "tracker directory",
		namesDirectory: true, underMigrationsDir: true,
		envelopeHandles: func(e migrationRecordEnvelope) []string { return []string{e.Subject.TrackerDir} },
	},
	{
		field:          string(migrationRoleStaged),
		dirs:           func(e migrationRecordEnvelope) map[string]string { return e.Subject.dirsInRole(migrationStagedOf) },
		namesDirectory: true, shape: migrationShapeSidecar,
	},
	{
		field: "property",
		envelopeHandles: func(e migrationRecordEnvelope) []string {
			return slices.Concat(
				e.Subject.Properties(),
				slices.Sorted(maps.Keys(e.displacedDirs())),
				e.flippedProps())
		},
	},
	{
		field:          string(migrationRoleCanonical),
		dirs:           func(e migrationRecordEnvelope) map[string]string { return e.Subject.dirsInRole(migrationCanonicalOf) },
		namesDirectory: true, shape: migrationShapePropertyBucket,
	},
	{
		field:          string(migrationRoleSidecar),
		dirs:           func(e migrationRecordEnvelope) map[string]string { return e.Subject.dirsInRole(migrationSidecarOf) },
		namesDirectory: true, shape: migrationShapeSidecar,
	},
	{
		field:              "displaced directory",
		dirs:               func(e migrationRecordEnvelope) map[string]string { return e.displacedDirs() },
		namesDirectory:     true,
		shape:              migrationShapePropertyBucket,
		displacesCanonical: true,
	},
}

func migrationRolesWithShape(shape migrationHandleShape) []migrationDirRole {
	var out []migrationDirRole
	for _, group := range migrationHandleGroups {
		if group.shape == shape {
			out = append(out, migrationDirRole(group.field))
		}
	}
	return out
}

func (e migrationRecordEnvelope) displacedDirs() map[string]string {
	if e.Flip == nil {
		return nil
	}
	return e.Flip.DisplacedDirs
}

func (e migrationRecordEnvelope) flippedProps() []string {
	if e.Flip == nil {
		return nil
	}
	return e.Flip.Flipped
}

func validateMigrationHandles(e migrationRecordEnvelope) error {
	for _, group := range migrationHandleGroups {
		for _, handle := range group.handles(e) {
			if handle == "" && group.namesDirectory {
				continue
			}
			if !migrationHandleIsOneElement(handle) {
				return fmt.Errorf("record %q names %s %q, which is not a single directory inside the shard",
					e.Subject.Key, group.field, handle)
			}
			if group.namesDirectory && migrationReservedDirName(handle) {
				return fmt.Errorf("record %q names %s %q, which is a directory no migration may own",
					e.Subject.Key, group.field, handle)
			}
			switch group.shape {
			case migrationShapeUnchecked:
			case migrationShapeSidecar:
				if !migrationHandleIsSidecarShaped(handle) {
					return fmt.Errorf("record %q names %s %q, which is not shaped like a sidecar of a property bucket",
						e.Subject.Key, group.field, handle)
				}
			case migrationShapePropertyBucket:
				if !strings.HasPrefix(handle, migrationPropertyBucketPrefix) {
					return fmt.Errorf("record %q names %s %q, which is not a property bucket",
						e.Subject.Key, group.field, handle)
				}
			}
		}
	}
	return nil
}

// ".migrations"/"records" would let a teardown remove the tracker/record
// store; "objects" is the shard's whole object store.
func migrationReservedDirName(h string) bool {
	return h == migrationsDir || h == migrationRecordsDirName || h == helpers.ObjectsBucketLSM
}

// A positive shape rule, not a denylist, so it refuses stores added later
// too. Deliberately as weak as [isSidecarDirOf]: weaviate/weaviate#12621.
func migrationHandleIsSidecarShaped(h string) bool {
	tail, ok := strings.CutPrefix(h, migrationPropertyBucketPrefix)
	if !ok {
		return false
	}
	i := strings.Index(tail, "__")
	if i < 0 {
		return false
	}
	return slices.Contains(sidecarRoleWords, sidecarRoleWord(tail[i+2:]))
}

// TestEveryPropertyBucketCarriesTheMigrationPrefix pins this against the
// helpers that build those names.
const migrationPropertyBucketPrefix = "property_"

type migrationBlocks struct {
	checkpoint bool
	flip       bool
}

func (e migrationRecordEnvelope) requireBlocks(want migrationBlocks) error {
	if (e.Checkpoint != nil) != want.checkpoint {
		return fmt.Errorf("record %q in state %q: checkpoint block present=%v, wanted=%v",
			e.Subject.Key, e.State, e.Checkpoint != nil, want.checkpoint)
	}
	if (e.Flip != nil) != want.flip {
		return fmt.Errorf("record %q in state %q: flip block present=%v, wanted=%v",
			e.Subject.Key, e.State, e.Flip != nil, want.flip)
	}
	return nil
}
