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

package aggregator

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/aggregation"
)

func newBoolAggregator() *boolAggregator {
	return &boolAggregator{}
}

type boolAggregator struct {
	countTrue  uint64
	countFalse uint64
}

func (a *boolAggregator) AddBoolRow(value []byte, count uint64) error {
	var valueParsed bool

	if err := binary.Read(bytes.NewReader(value), binary.LittleEndian,
		&valueParsed); err != nil {
		return errors.Wrap(err, "read bool")
	}

	if count == 0 {
		// skip
		return nil
	}

	if valueParsed {
		a.countTrue += count
	} else {
		a.countFalse += count
	}

	return nil
}

func (a *boolAggregator) AddBool(value bool) error {
	if value {
		a.countTrue++
	} else {
		a.countFalse++
	}

	return nil
}

func (a *boolAggregator) Res() aggregation.Boolean {
	out := aggregation.Boolean{}

	count := int(a.countTrue) + int(a.countFalse)
	if count == 0 {
		return out
	}

	out.Count = count
	out.TotalFalse = int(a.countFalse)
	out.TotalTrue = int(a.countTrue)
	out.PercentageTrue = float64(a.countTrue) / float64(count)
	out.PercentageFalse = float64(a.countFalse) / float64(count)

	return out
}
