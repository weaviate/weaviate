//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/usecases/objects"
)

type result[T any] struct {
	data T
	err  error
}

type tuple[T any] struct {
	sender string
	UTime  int64
	o      T
	ack    int
	err    error
}

type objTuple tuple[objects.Replica]

type boolTuple tuple[bool]

func readOneExists(ch <-chan simpleResult[existReply], st rState) (bool, error) {
	counters := make([]boolTuple, 0, len(st.Hosts))
	for r := range ch {
		resp := r.Response
		if r.Err != nil {
			counters = append(counters, boolTuple{resp.sender, 0, false, 0, r.Err})
			continue
		}
		counters = append(counters, boolTuple{resp.sender, resp.UpdateTime, resp.Data, 0, nil})
		max := 0
		for i := range counters {
			if r.Err == nil && counters[i].o == resp.Data {
				counters[i].ack++
			}
			if max < counters[i].ack {
				max = counters[i].ack
			}
			if max >= st.Level {
				return counters[i].o, nil
			}
		}
	}

	var sb strings.Builder
	for i, c := range counters {
		if i != 0 {
			sb.WriteString(", ")
		}
		if c.err != nil {
			fmt.Fprintf(&sb, "%s: %s", c.sender, c.err.Error())
		} else {
			fmt.Fprintf(&sb, "%s: %t", c.sender, c.o)
		}
	}
	return false, fmt.Errorf("%w %q %s", ErrConsistencyLevel, st.CLevel, sb.String())
}
