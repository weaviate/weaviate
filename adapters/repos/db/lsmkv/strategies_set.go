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

type setDecoder struct{}

func newSetDecoder() *setDecoder {
	return &setDecoder{}
}

func (s *setDecoder) Do(in []value) [][]byte {
	// check if there are tombstones, if not, we can simply take the list without
	// further processing
	var tombstones int
	for _, value := range in {
		if value.tombstone {
			tombstones++
		}
	}

	if tombstones == 0 {
		return s.doWithoutTombstones(in)
	}

	// there are tombstones, we need to remove them
	// TODO: The logic below can be improved since don't care about the "latest"
	// write on a set, as all writes are per definition identical. Any write that
	// is not followed by a tombstone is fine
	count := make(map[string]uint, len(in))
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

func (s *setDecoder) doWithoutTombstones(in []value) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = in[i].value
	}

	// take an arbitrary cutoff for when it is worth to remove duplicates. The
	// assumption is that on larger lists, duplicates are more likely to be
	// tolerated, for example, because the point is to build an allow list for a
	// secondary index where a duplicate does not matter. If the amount is
	// smaller than the cutoff this is more likely to be relevant to a user.
	//
	// As the list gets longer, removing duplicates gets a lot more expensive,
	// hence it makes sense to skip the de-duplication, if we can be reasonably
	// sure that it does not matter
	if len(out) <= 1000 {
		return s.deduplicateResults(out)
	}

	return out
}

func (s *setDecoder) deduplicateResults(in [][]byte) [][]byte {
	out := make([][]byte, len(in))

	seen := map[string]struct{}{}

	i := 0
	for _, elem := range in {
		if _, ok := seen[string(elem)]; ok {
			continue
		}

		out[i] = elem
		seen[string(elem)] = struct{}{}
		i++
	}

	return out[:i]
}

// DoPartial keeps any extra tombstones, but does not keep tombstones which
// were "consumed"
func (s *setDecoder) DoPartial(in []value) []value {
	count := map[string]uint{}
	for _, value := range in {
		count[string(value.value)] = count[string(value.value)] + 1
	}

	out := make([]value, len(in))

	i := 0
	for _, value := range in {
		if count[string(value.value)] != 1 {
			count[string(value.value)] = count[string(value.value)] - 1
			continue
		}

		out[i] = value
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
