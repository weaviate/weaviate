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
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSearchSegment(t *testing.T) {
	logger, _ := test.NewNullLogger()

	path := "/Users/amourao/segments/title"

	queryTerms := []string{"white", "button", "up", "shirt", "women", "s"}
	idfs := map[string]float64{
		"white":  3.004202,
		"button": 3.342705,
		"up":     3.260511,
		"shirt":  1.786043,
		"women":  1.027962,
		"s":      1.348126,
	}

	ids := map[string]int{
		"white":  0,
		"button": 1,
		"up":     2,
		"shirt":  3,
		"women":  4,
		"s":      5,
	}

	// load all segments from folder in disk
	dir, err := os.ReadDir(path)
	if err != nil {
		t.Errorf("Error reading folder: %v", err)
	}

	segments := make([]*segment, 0)

	for _, file := range dir {
		segPath := path + "/" + file.Name()
		if strings.HasSuffix(file.Name(), ".db.inverted") {
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
			segment.loadTombstones()
			segment.loadPropertyLengths()

			fmt.Printf("Segment %s loaded\n", segPath)
			segments = append(segments, segment)

		}
	}

	averagePropLength := 6.959282

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

	for i, segment := range segments {
		for k := 0; k < 10; k++ {
			termss := make([]*SegmentBlockMax, len(queryTerms))
			termss2 := make([]terms.TermInterface, len(queryTerms))
			for j, key := range queryTerms {
				termss[j] = NewSegmentBlockMax(segment, []byte(key), ids[key], idfs[key], 1, nil, nil, averagePropLength, config)
				termss2[j] = termss[j]
			}
			start := time.Now()
			// termsss := &terms.Terms{
			// T:     termss2,
			// 	Count: len(termss),
			// }
			// results := terms.DoBlockMaxWand(110, termsss, averagePropLength, false)
			results := DoBlockMaxWand(110, termss, averagePropLength, false, len(termss))
			elapsed := time.Since(start)
			if k == 0 {
				for results.Len() > 0 {
					result := results.Pop()
					fmt.Printf("%v %v\n", result.ID, result.Dist)
				}
				fmt.Printf("\n")
			}

			for _, segTer := range termss {
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

			fmt.Printf("Segment %d took %s\n", i, elapsed)

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
