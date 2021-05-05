//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

type setDecoder struct{}

func newSetDecoder() *setDecoder {
	return &setDecoder{}
}

func (s *setDecoder) Do(in []value) [][]byte {
	count := map[string]uint{}
	for _, value := range in {
		count[string(value.value)] = count[string(value.value)] + 1
	}

	out := make([][]byte, len(in))

	i := 0
	for _, value := range in {
		if count[string(value.value)] != 1 {
			count[string(value.value)] = count[string(value.value)] - 1
			continue
		}

		if value.tombstone {
			continue
		}

		out[i] = value.value
		i++
	}

	return out[:i]
}

type setEncoder struct{}

func newSetEncoder() *setEncoder {
	return &setEncoder{}
}

func (s *setEncoder) Do(in [][]byte) []value {
	out := make([]value, len(in))
	for i, v := range in {
		out[i] = value{
			tombstone: false,
			value:     v,
		}
	}

	return out
}
