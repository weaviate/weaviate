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

package postingedge

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	entcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const (
	// DefaultPostingBudgetKB is the size budget of one posting, the same
	// budget HFresh gives a posting. With doc id + centered 1-bit code
	// entries it yields ~236 vectors per posting at 1536 dimensions.
	DefaultPostingBudgetKB = 48
	// DefaultEdges is the number of nearest neighbors considered per vector;
	// the ones living in another posting become its edges.
	DefaultEdges = 16
	// DefaultBuildProbe is how many of a vector's nearest centroids the edge
	// builder draws neighbor candidates from.
	DefaultBuildProbe = 256
	// DefaultRescueLimit is how many code-ranked neighbor candidates the edge
	// builder re-ranks with exact distances.
	DefaultRescueLimit = 64
	// DefaultEntryPostings is the number of postings a search starts from.
	DefaultEntryPostings = 1
	// DefaultLoadBatch is the number of postings loaded per round.
	DefaultLoadBatch = 4
	// DefaultEF is the smallest ef of the benchmark sweep that reaches a
	// recall@10 of 0.95 with the other defaults (0.9502 on dbpedia-openai
	// 1M). At ef = k the search stops as soon as no known unloaded posting
	// can improve the top k, which finds 0.92.
	DefaultEF = 20
	// SourceMarginAuto asks for the source margin calibrated for the
	// distance of the index: DefaultCosineSourceMargin for cosine, and none
	// for l2-squared, for which there is no calibration yet. A margin is in
	// the units of the distance, so one calibrated for cosine means nothing
	// for another distance.
	SourceMarginAuto    = -1
	DefaultSourceMargin = SourceMarginAuto
	// DefaultCosineSourceMargin is the margin whose recall/QPS frontier was
	// ahead of no margin everywhere on dbpedia-openai 1M: 41% to 64% more
	// QPS at equal recall, for at most 0.003 of recall at equal ef.
	DefaultCosineSourceMargin = 0.06
	// DefaultRescoreLimit is far below the 512 HNSW uses for 1-bit RQ: the
	// codes rank the top well enough that 64 measured the same recall as
	// 512, which costs half a millisecond per query.
	DefaultRescoreLimit = 64
	// DefaultTrainingLimit matches what HNSW uses for centered RQ.
	DefaultTrainingLimit = enthnsw.DefaultRQTrainingLimit

	// centroidEF is the ef of the centroid graph search. Only the few
	// nearest centroids are ever requested.
	centroidEF = 64
	// docIDBytes is the size of one doc id in a posting entry.
	docIDBytes = 8
)

// UserConfig holds the tunables of the index. It is the part that would move
// to entities/vectorindex once the index is wired into the schema.
type UserConfig struct {
	Distance string `json:"distance"`
	// TargetPostingSize is the mean number of vectors per posting. 0 derives
	// it from DefaultPostingBudgetKB and the size of the codes in the
	// postings, so it grows where CodeBits or ScanCodeBits shorten them.
	TargetPostingSize int `json:"targetPostingSize"`
	// CodeBits caps the sign bits kept per code, a multiple of 64. Fewer
	// bits make the codes smaller and the distance estimates noisier, for
	// the scan, the ranking of endpoints and the edge builder alike. 0
	// keeps one bit per dimension, the full code, and is the default.
	//
	// Half of the bits is the memory mode: 768 at 1536 dimensions. It takes
	// more than this field to get what was measured for it, see "Code
	// budget" in the package documentation.
	CodeBits int `json:"codeBits"`
	// ScanCodeBits gives the postings codes of their own for the scan, capped
	// to this many sign bits, a multiple of 64. The ranking of endpoints and
	// the edge builder keep the codes of CodeBits, which then live in the
	// sidecar only: the index holds two codes per vector, so this buys
	// shorter scans and not memory. It is the layout of a disk-resident
	// version, where the postings are what is read from disk. 0 scans the
	// codes of CodeBits, held once.
	//
	// EF does not mean the same with scan codes: the stop threshold is the
	// EF-th best scan estimate, and scan estimates are noisier than the
	// sidecar estimates the postings are ranked by. The search stops earlier
	// at the same EF, so the same work takes a larger one.
	ScanCodeBits int `json:"scanCodeBits"`
	// Edges, BuildProbe, RescueLimit and TrainingLimit shape the build and
	// cannot change once the index is built, like the three fields above.
	Edges         int `json:"edges"`
	BuildProbe    int `json:"buildProbe"`
	RescueLimit   int `json:"rescueLimit"`
	TrainingLimit int `json:"trainingLimit"`
	// EntryPostings, LoadBatch, EF and RescoreLimit apply per search and can
	// be updated at any time.
	EntryPostings int `json:"entryPostings"`
	LoadBatch     int `json:"loadBatch"`
	// EF is the quality knob: the search stops when the best known unloaded
	// posting is no closer than the EF-th candidate. Values below k are
	// raised to k.
	EF           int `json:"ef"`
	RescoreLimit int `json:"rescoreLimit"`
	// SourceMargin cuts the expansion: only the edges of the scanned vectors
	// within this distance of the stop threshold are followed. Vectors
	// further from the query rarely link to anything the search still
	// needs. It is a heuristic that trades recall for work, in the units of
	// the distance of the index. 0 follows the edges of every scanned
	// vector, SourceMarginAuto takes the margin calibrated for the distance.
	SourceMargin float32 `json:"sourceMargin"`
}

func NewDefaultUserConfig() UserConfig {
	return UserConfig{
		Distance:      entcommon.DistanceCosine,
		Edges:         DefaultEdges,
		BuildProbe:    DefaultBuildProbe,
		RescueLimit:   DefaultRescueLimit,
		TrainingLimit: DefaultTrainingLimit,
		EntryPostings: DefaultEntryPostings,
		LoadBatch:     DefaultLoadBatch,
		EF:            DefaultEF,
		RescoreLimit:  DefaultRescoreLimit,
		SourceMargin:  DefaultSourceMargin,
	}
}

func (u UserConfig) IndexType() string {
	return common.IndexTypePostingEdge
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

func (u UserConfig) Validate() error {
	ec := errorcompounder.New()

	if u.TargetPostingSize < 0 {
		ec.Addf("targetPostingSize must not be negative, got %d", u.TargetPostingSize)
	}
	if u.CodeBits < 0 || u.CodeBits%64 != 0 {
		ec.Addf("codeBits must be 0 or a positive multiple of 64, got %d", u.CodeBits)
	}
	if u.ScanCodeBits < 0 || u.ScanCodeBits%64 != 0 {
		ec.Addf("scanCodeBits must be 0 or a positive multiple of 64, got %d", u.ScanCodeBits)
	}
	// Against full codes the width is only known at build time, which then
	// rejects scan codes wider than the code.
	if u.CodeBits > 0 && u.ScanCodeBits >= u.CodeBits {
		ec.Addf("scanCodeBits (%d) must be smaller than codeBits (%d)", u.ScanCodeBits, u.CodeBits)
	}
	if u.Edges < 1 {
		ec.Addf("edges must be at least 1, got %d", u.Edges)
	}
	if u.BuildProbe < 1 {
		ec.Addf("buildProbe must be at least 1, got %d", u.BuildProbe)
	}
	if u.RescueLimit < u.Edges {
		ec.Addf("rescueLimit (%d) must not be smaller than edges (%d)", u.RescueLimit, u.Edges)
	}
	if u.TrainingLimit < 1 {
		ec.Addf("trainingLimit must be at least 1, got %d", u.TrainingLimit)
	}
	if u.EntryPostings < 1 {
		ec.Addf("entryPostings must be at least 1, got %d", u.EntryPostings)
	}
	if u.LoadBatch < 1 {
		ec.Addf("loadBatch must be at least 1, got %d", u.LoadBatch)
	}
	if u.EF < 0 {
		ec.Addf("ef must not be negative, got %d", u.EF)
	}
	if u.RescoreLimit < 0 {
		ec.Addf("rescoreLimit must not be negative, got %d", u.RescoreLimit)
	}
	// Written so that NaN fails too.
	if !(u.SourceMargin >= 0) && u.SourceMargin != SourceMarginAuto {
		ec.Addf("sourceMargin must not be negative, got %v", u.SourceMargin)
	}

	return ec.ToError()
}

// Config holds what the owner of the index (the shard, a test, a benchmark)
// derives internally.
type Config struct {
	ID        string
	ShardName string
	ClassName string
	// RootPath is handed to the centroid graph. This version persists
	// nothing below it.
	RootPath         string
	Logger           logrus.FieldLogger
	DistanceProvider distancer.Provider
	AllocChecker     memwatch.AllocChecker
	// TempVectorForIDWithViewThunk and GetViewThunk read the full-precision
	// vectors a search rescores against, exactly as HNSW reads them.
	TempVectorForIDWithViewThunk common.TempVectorForIDWithView[float32]
	GetViewThunk                 common.GetViewThunk
	// Seed makes the build reproducible. The partitioning is the only
	// randomized step.
	Seed uint64
}

func (c *Config) validate() error {
	ec := errorcompounder.New()

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}
	if c.RootPath == "" {
		ec.Addf("rootPath cannot be empty")
	}
	if c.DistanceProvider == nil {
		ec.Addf("distanceProvider cannot be nil")
	}
	if c.TempVectorForIDWithViewThunk == nil {
		ec.Addf("tempVectorForIDWithViewThunk cannot be nil")
	}
	if c.GetViewThunk == nil {
		ec.Addf("getViewThunk cannot be nil")
	}
	if err := ec.ToError(); err != nil {
		return err
	}

	switch c.DistanceProvider.Type() {
	case "cosine-dot", "l2-squared":
	default:
		// The partitioning and the centroid graph work in l2 space, which
		// agrees with cosine on normalized vectors but not with a raw dot
		// product.
		return errors.Errorf("distance %q is not supported, use cosine or l2-squared",
			c.DistanceProvider.Type())
	}

	c.Logger = common.LoggerOrDiscard(c.Logger)
	if c.AllocChecker == nil {
		c.AllocChecker = memwatch.NewDummyMonitor()
	}
	return nil
}

// targetPostingSize resolves the configured mean posting size for codes of
// the given size.
func targetPostingSize(configured, codeBytes int) int {
	if configured > 0 {
		return configured
	}
	return max(1, DefaultPostingBudgetKB*1024/(docIDBytes+codeBytes))
}
