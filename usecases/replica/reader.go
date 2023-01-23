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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

type tuple[T any] struct {
	sender string
	o      T
	ack    int
	err    error
}

type objTuple tuple[*storobj.Object]

func readOne(ch <-chan simpleResult[findOneReply], cl int) (*storobj.Object, error) {
	counters := make([]objTuple, 0, cl*2)
	N, nnf := 0, 0
	for r := range ch {
		N++
		resp := r.Response
		if r.Err != nil {
			counters = append(counters, objTuple{resp.sender, nil, 0, r.Err})
			continue
		} else if resp.data == nil {
			nnf++
			continue
		}
		counters = append(counters, objTuple{resp.sender, resp.data, 0, nil})
		lastTime := resp.data.LastUpdateTimeUnix()
		max := 0
		for i := range counters {
			if counters[i].o != nil && counters[i].o.LastUpdateTimeUnix() == lastTime {
				counters[i].ack++
			}
			if max < counters[i].ack {
				max = counters[i].ack
			}
			if max >= cl {
				return counters[i].o, nil
			}
		}
	}
	if nnf == N { // object doesn't exist
		return nil, nil
	}

	var sb strings.Builder
	for i, c := range counters {
		if i != 0 {
			sb.WriteString(", ")
		}
		if c.err != nil {
			fmt.Fprintf(&sb, "%s: %s", c.sender, c.err.Error())
		} else if c.o == nil {
			fmt.Fprintf(&sb, "%s: 0", c.sender)
		} else {
			fmt.Fprintf(&sb, "%s: %d", c.sender, c.o.LastUpdateTimeUnix())
		}
	}
	return nil, errors.New(sb.String())
}

type boolTuple tuple[bool]

func readOneExists(ch <-chan simpleResult[existReply], cl int) (bool, error) {
	counters := make([]boolTuple, 0, cl*2)
	for r := range ch {
		resp := r.Response
		if r.Err != nil {
			counters = append(counters, boolTuple{resp.sender, false, 0, r.Err})
			continue
		}
		counters = append(counters, boolTuple{resp.sender, resp.data, 0, nil})
		max := 0
		for i := range counters {
			if r.Err == nil && counters[i].o == resp.data {
				counters[i].ack++
			}
			if max < counters[i].ack {
				max = counters[i].ack
			}
			if max >= cl {
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
	return false, errors.New(sb.String())
}
