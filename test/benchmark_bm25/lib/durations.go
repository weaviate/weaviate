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

package lib

import (
	"fmt"
	"sort"
	"time"
)

type LatencyStats struct {
	Min  time.Duration
	Max  time.Duration
	Mean time.Duration
	P50  time.Duration
	P90  time.Duration
	P99  time.Duration
}

func AnalyzeLatencies(in []time.Duration) LatencyStats {
	out := LatencyStats{}
	sort.Slice(in, func(a, b int) bool { return in[a] < in[b] })

	out.Min = in[0]
	out.Max = in[len(in)-1]

	sum := time.Duration(0)
	for _, dur := range in {
		sum += dur
	}
	out.Mean = sum / time.Duration(len(in))

	out.P50 = in[len(in)/2]
	out.P90 = in[len(in)*9/10]
	out.P99 = in[len(in)*99/100]

	return out
}

func (l LatencyStats) PrettyPrint() {
	fmt.Printf("\n")
	fmt.Printf("Query Latencies \n")
	fmt.Printf("-------------------- \n")
	fmt.Printf("Min:  %12s\n", l.Min)
	fmt.Printf("Mean: %12s\n", l.Mean)
	fmt.Printf("Max:  %12s\n", l.Max)
	fmt.Printf("p50:  %12s\n", l.P50)
	fmt.Printf("p90:  %12s\n", l.P90)
	fmt.Printf("p99:  %12s\n", l.P99)
	fmt.Printf("\n")
}
