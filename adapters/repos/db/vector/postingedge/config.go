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
	// DefaultEF of 0 follows k: the search stops as soon as no known
	// unloaded posting can improve the top k.
	DefaultEF = 0
	// DefaultRescoreLimit and DefaultTrainingLimit match what HNSW uses for
	// 1-bit RQ, so both indexes quantize and rescore alike by default.
	DefaultRescoreLimit  = enthnsw.DefaultBRQRescoreLimit
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
	// it from the dimensionality and DefaultPostingBudgetKB.
	TargetPostingSize int `json:"targetPostingSize"`
	// Edges, BuildProbe, RescueLimit and TrainingLimit shape the build and
	// cannot change once the index is built.
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
