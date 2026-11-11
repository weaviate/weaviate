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
	"context"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

// Build turns the vectors added so far into a searchable index. It runs once:
// the index cannot change afterwards, and it releases the full-precision
// vectors it was holding for the build.
//
// Searches may run concurrently with Build. They fail with ErrNotBuilt until
// it returns.
func (ix *Index) Build(ctx context.Context) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	if ix.closed {
		return errors.New("posting-edge index is shut down")
	}
	if ix.pending == nil {
		return ErrBuilt
	}
	if len(ix.pending.ids) == 0 {
		return errors.New("cannot build a posting-edge index without vectors")
	}

	st, err := ix.build(ctx, ix.pending, ix.uc)
	if err != nil {
		return errors.Wrap(err, "build posting-edge index")
	}
	ix.state.Store(st)
	ix.pending = nil
	return nil
}

func (ix *Index) build(ctx context.Context, input *pendingVectors, uc UserConfig) (*state, error) {
	begin := time.Now()
	vectors := input.vectors
	n := len(vectors)
	dims := int(ix.dims.Load())
	stats := BuildStats{Vectors: n}

	stage := time.Now()
	quantizer, err := trainQuantizer(vectors, dims, uc.TrainingLimit, uc.CodeBits, &ix.cfg)
	if err != nil {
		return nil, err
	}
	// The postings hold the codes the scan reads: those of the sidecar, or
	// shorter ones of their own over the same quantizer.
	var scanQuantizer codec
	inPostings := quantizer
	if uc.ScanCodeBits != 0 {
		scanQuantizer, err = quantizer.withBits(uc.ScanCodeBits)
		if err != nil {
			return nil, errors.Wrap(err, "scan codes")
		}
		inPostings = scanQuantizer
	}
	trainTime := time.Since(stage)

	// The posting size is a byte budget, so the number of postings follows
	// from the size of the codes the postings hold.
	codeBytes := 8 * len(inPostings.Encode(vectors[0]))
	target := targetPostingSize(uc.TargetPostingSize, codeBytes)
	postings := int(math.Ceil(float64(n) / float64(target)))

	var centroids [][]float32
	var assigned *assignment
	if shared := ix.shared; shared != nil && shared.filled() {
		if err := shared.fits(n, postings, uc.BuildProbe); err != nil {
			return nil, errors.Wrap(err, "shared partition and assignment")
		}
		centroids, assigned = shared.centroids, shared.assigned
		ix.logStage("partition and assign taken from another build", 0, logrus.Fields{"postings": len(centroids)})
	} else {
		stage = time.Now()
		centroids, err = partition(ctx, ix.logger, vectors, dims, postings, ix.cfg.Seed)
		if err != nil {
			return nil, errors.Wrap(err, "partition vectors")
		}
		stats.PartitionTime = time.Since(stage)
		ix.logStage("partition", stats.PartitionTime, logrus.Fields{"postings": len(centroids)})

		stage = time.Now()
		assigned, err = assign(ctx, ix.logger, vectors, centroids, uc.BuildProbe)
		if err != nil {
			return nil, errors.Wrap(err, "assign vectors to centroids")
		}
		centroids = dropEmpty(centroids, assigned)
		stats.AssignTime = time.Since(stage)
		ix.logStage("assign", stats.AssignTime, logrus.Fields{"postings": len(centroids)})
		if ix.shared != nil {
			*ix.shared = buildShare{vectors: n, postings: postings, probe: uc.BuildProbe, centroids: centroids, assigned: assigned}
		}
	}
	layout := newSlotLayout(assigned.primary, len(centroids))
	stats.Postings = len(centroids)

	stage = time.Now()
	codes, codeWords, err := encode(ctx, ix.logger, vectors, layout, quantizer)
	if err != nil {
		return nil, errors.Wrap(err, "encode vectors")
	}
	side := &sidecar{codeWords: codeWords, codes: codes, postingOf: postingOfSlots(layout.start)}
	scanCodes, scanWords := codes, codeWords
	if scanQuantizer != nil {
		scanCodes, scanWords, err = encode(ctx, ix.logger, vectors, layout, scanQuantizer)
		if err != nil {
			return nil, errors.Wrap(err, "encode vectors for the scan")
		}
	}
	stats.EncodeTime = trainTime + time.Since(stage)
	ix.logStage("encode", stats.EncodeTime, nil)

	stage = time.Now()
	edgeStart, edges, scored, err := buildEdges(ctx, ix.logger, edgeInput{
		vectors:   vectors,
		layout:    layout,
		assign:    assigned,
		side:      side,
		quantizer: quantizer,
		provider:  ix.provider,
		probe:     uc.BuildProbe,
		neighbors: uc.Edges,
		rescue:    uc.RescueLimit,
	})
	if err != nil {
		return nil, errors.Wrap(err, "build edges")
	}
	stats.EdgeTime = time.Since(stage)
	stats.Edges = len(edges)
	stats.CandidatesScored = scored
	ix.logStage("edges", stats.EdgeTime, logrus.Fields{"edges": len(edges)})

	docIDs := make([]uint64, n)
	for slot, inputPos := range layout.order {
		docIDs[slot] = input.ids[inputPos]
	}

	stage = time.Now()
	graph, err := newCentroidIndex(ctx, &ix.cfg, ix.dir(), centroids)
	if err != nil {
		return nil, err
	}
	stats.CentroidGraphTime = time.Since(stage)
	stats.TotalTime = time.Since(begin)
	ix.logStage("centroid_graph", stats.CentroidGraphTime, nil)

	st := &state{
		dims:          dims,
		quantizer:     quantizer,
		scanQuantizer: scanQuantizer,
		centroids:     graph,
		postings: &postingStore{
			codeWords: scanWords,
			start:     layout.start,
			docIDs:    docIDs,
			codes:     scanCodes,
			edgeStart: edgeStart,
			edges:     edges,
		},
		side:    side,
		visited: visited.NewPool(n),
		build:   stats,
	}
	st.scratch.New = func() any { return newSearchScratch(len(centroids)) }

	ix.logger.WithFields(logrus.Fields{
		"action":   "posting_edge_build",
		"vectors":  n,
		"postings": len(centroids),
		"edges":    len(edges),
		"took":     stats.TotalTime.String(),
	}).Info("posting-edge index built")
	return st, nil
}

// buildShare hands the partition and the assignment of one build to another.
// They follow from the vectors, the number of postings and the probe width
// alone, and at ten million vectors they take hours: a benchmark that builds
// several indexes over one corpus, apart in their codes only, computes them
// once. A build that finds them filled in takes them as they are, one that
// finds them empty fills them in. Neither build changes them afterwards.
//
// What was computed with a wider build probe serves a narrower build: the
// probes of a vector are its nearest centroids, nearest first, and the edge
// builder reads the head of the list.
type buildShare struct {
	// what the partition and the assignment were computed for
	vectors, postings, probe int
	centroids                [][]float32 // without the empty ones
	assigned                 *assignment // numbered accordingly
}

func (s *buildShare) filled() bool {
	return s.assigned != nil
}

// fits tells whether what is shared comes from a build like this one, or from
// one that differs in a wider build probe. Whether its vectors were the same
// ones it cannot tell: that is on whoever shares.
func (s *buildShare) fits(vectors, postings, probe int) error {
	if s.vectors != vectors || s.postings != postings || s.probe < probe {
		return errors.Errorf("computed for %d vectors in %d postings with a build probe of %d, "+
			"this build has %d vectors in %d postings with a build probe of %d",
			s.vectors, s.postings, s.probe, vectors, postings, probe)
	}
	a := s.assigned
	// The width is the probe it was computed with, or the number of
	// centroids there were where that is smaller.
	if len(a.primary) != vectors || a.width < 1 || a.width > s.probe || len(a.probes) != vectors*a.width {
		return errors.Errorf("assignment of %d vectors with %d probe entries of width %d does not hold together",
			len(a.primary), len(a.probes), a.width)
	}
	return nil
}

func (ix *Index) logStage(name string, took time.Duration, fields logrus.Fields) {
	ix.logger.WithFields(fields).WithFields(logrus.Fields{
		"action": "posting_edge_build",
		"stage":  name,
		"took":   took.String(),
	}).Debug("posting-edge build stage done")
}
