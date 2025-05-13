//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/fgprof"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSearchBMW(t *testing.T) {
	logger, _ := test.NewNullLogger()

	// path := "/Users/amourao/property_d_description_searchable_oldImpact"
	path := "/Users/amourao/property_d_description_searchable_fixedImpact2/"

	queryTerms := []string{"white", "button", "up", "shirt", "women", "s"}
	// queryTerms := []string{"white", "button"}

	idfs := map[string]float64{
		"up":     3.260511,
		"shirt":  1.786043,
		"women":  1.027962,
		"s":      1.348126,
		"white":  3.004202,
		"button": 3.342705,
	}

	ids := map[string]int{
		"white":  0,
		"button": 1,
		"up":     2,
		"shirt":  3,
		"women":  4,
		"s":      5,
	}
	/*
		path = "/Users/amourao/code/weaviate/weaviate/data-inverted_newPropLen/nfcorpus_test/N4pDSZ6ujEs8/lsm/property_title_searchable/"

		queryTerms = []string{"at"}
		ids = map[string]int{
			"at": 0,
		}

		idfs = map[string]float64{
			"at": 1,
		}
	*/

	// load all segments from folder in disk
	dir, err := os.ReadDir(path)
	if err != nil {
		t.Errorf("Error reading folder: %v", err)
	}

	segments := make([]*segment, 0)

	for _, file := range dir {
		segPath := path + "/" + file.Name()
		if strings.HasSuffix(segPath, ".db") {
			fmt.Printf("Loading segment %s\n", segPath)
			segment, err := newSegment(segPath, logger, nil, nil, segmentConfig{
				mmapContents:             false,
				useBloomFilter:           true,
				calcCountNetAdditions:    false,
				overwriteDerived:         false,
				enableChecksumValidation: false,
			})
			if err != nil {
				t.Errorf("Error creating segment: %v", err)
			}
			fmt.Printf("Segment %s loaded\n", segPath)
			segments = append(segments, segment)

		}
	}

	averagePropLength := 101.764565
	// averagePropLength = 40

	config := schema.BM25Config{
		K1: 1.2,
		B:  0.75,
	}

	f, err := os.OpenFile("fgprof.perf", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
		return
	}

	f2, err := os.OpenFile("pprof.perf", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
		return
	}
	err = pprof.StartCPUProfile(f2)
	if err != nil {
		t.Errorf("Error starting trace: %v", err)
		return
	}

	// io writer to file
	stop := fgprof.Start(f, fgprof.FormatPprof, []string{})
	iterations := 1
	for i, segment := range segments {
		allResults := make([]*priorityqueue.Queue[[]*terms.DocPointerWithScore], iterations)
		for k := 0; k < iterations; k++ {
			start := time.Now()
			termsss := make([]*SegmentBlockMax, 0, len(queryTerms))
			for _, key := range queryTerms {
				seg := NewSegmentBlockMax(segment, []byte(key), ids[key], idfs[key], 1, nil, nil, averagePropLength, config)
				if seg != nil {
					termsss = append(termsss, seg)
				}
			}

			allResults[k] = DoBlockMaxWand(110, termsss, averagePropLength, true, len(queryTerms), 1)
			elapsed := time.Since(start)
			fmt.Printf("Segment %d took %s\n", i, elapsed)

			for _, segTer := range termsss {
				/*
					BlockCountTotal         uint64
					BlockCountDecodedDocIds uint64
					BlockCountDecodedFreqs  uint64
					DocCountTotal           uint64
					DocCountDecodedDocIds   uint64
					DocCountDecodedFreqs    uint64
					DocCountScored          uint64
					QueryCount              uint64
					LastAddedBlock          int
				*/
				fmt.Printf("%s;%d;%d;%d;%d;%d;%d;%d\n", string(segTer.node.Key), segTer.Metrics.BlockCountTotal, segTer.Metrics.BlockCountDecodedDocIds, segTer.Metrics.BlockCountDecodedFreqs, segTer.Metrics.DocCountTotal, segTer.Metrics.DocCountDecodedDocIds, segTer.Metrics.DocCountDecodedFreqs, segTer.Metrics.DocCountScored)
			}

		}
		gt := make([]uint64, 110)
		gtScores := make([]float64, 110)
		for i, results := range allResults {
			ids := make([]uint64, results.Len())
			scores := make([]float64, results.Len())
			for results.Len() > 0 {
				result := results.Pop()
				if i == 0 {
					gt[results.Len()] = result.ID
					gtScores[results.Len()] = float64(result.Dist)
				}
				ids[results.Len()] = result.ID
				scores[results.Len()] = float64(result.Dist)
				explanations := ""
				propLen := float32(0)
				for j, explanation := range result.Value {
					if explanation == nil {
						continue
					}
					explanations += fmt.Sprintf("%v %v; ", queryTerms[j], explanation.Frequency)
					propLen = explanation.PropLength
				}
				explanations += fmt.Sprintf("propLen: %v", propLen)
				fmt.Printf("%v %v %v\n", result.ID, result.Dist, explanations)
			}
			fmt.Printf("\n")
			// fmt.Printf("Results: %v\n", ids)
			// fmt.Printf("Scores: %v\n", scores)

			// assert result and scores equals to ground truth
			// assert.Equal(t, gt, ids)
			// assert.Equal(t, gtScores, scores)

		}
	}

	err = stop()
	if err != nil {
		t.Errorf("Error stopping fgprof: %v", err)
		return
	}

	pprof.StopCPUProfile()

	err = f.Close()
	if err != nil {
		t.Errorf("Error closing file: %v", err)
		return
	}

	err = f2.Close()
	if err != nil {
		t.Errorf("Error closing file: %v", err)
		return
	}
}
