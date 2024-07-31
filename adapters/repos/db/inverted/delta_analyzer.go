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

package inverted

import "bytes"

type DeltaResults struct {
	ToDelete []Property
	ToAdd    []Property
}

func Delta(previous, next []Property) DeltaResults {
	out := DeltaResults{}

	previous = DedupItems(previous)
	next = DedupItems(next)

	if previous == nil {
		out.ToAdd = next
		return out
	}

	previousByProp := map[string]Property{}
	for _, prevProp := range previous {
		previousByProp[prevProp.Name] = prevProp
	}

	for _, nextProp := range next {
		prevProp, ok := previousByProp[nextProp.Name]
		if !ok {
			// this prop didn't exist before so we can add all of it
			out.ToAdd = append(out.ToAdd, nextProp)
			continue
		}
		delete(previousByProp, nextProp.Name)

		// there is a chance they're identical, such a check is pretty cheap and
		// it could prevent us from running an expensive merge, so let's try our
		// luck
		if listsIdentical(prevProp.Items, nextProp.Items) {
			// then we don't need to do anything about this prop
			continue
		}

		toAdd, toDelete := countableDelta(prevProp.Items, nextProp.Items)
		if len(toAdd) > 0 {
			out.ToAdd = append(out.ToAdd, Property{
				Name:               nextProp.Name,
				Items:              toAdd,
				Length:             nextProp.Length,
				HasFilterableIndex: nextProp.HasFilterableIndex,
				HasSearchableIndex: nextProp.HasSearchableIndex,
				HasRangeableIndex:  nextProp.HasRangeableIndex,
			})
		}
		if len(toDelete) > 0 {
			out.ToDelete = append(out.ToDelete, Property{
				Name:               nextProp.Name,
				Items:              toDelete,
				Length:             prevProp.Length,
				HasFilterableIndex: nextProp.HasFilterableIndex,
				HasSearchableIndex: nextProp.HasSearchableIndex,
				HasRangeableIndex:  nextProp.HasRangeableIndex,
			})
		}
		// special case to update optional length/nil indexes on
		// all values removed
		if len(toAdd) == 0 && len(toDelete) > 0 &&
			nextProp.Length == 0 && prevProp.Length > 0 {
			out.ToAdd = append(out.ToAdd, Property{
				Name:               nextProp.Name,
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: nextProp.HasFilterableIndex,
				HasSearchableIndex: nextProp.HasSearchableIndex,
				HasRangeableIndex:  nextProp.HasRangeableIndex,
			})
		}
	}

	// extend ToDelete with props from previous missing in next
	for _, prevProp := range previous {
		if _, ok := previousByProp[prevProp.Name]; ok {
			out.ToDelete = append(out.ToDelete, prevProp)
		}
	}

	return out
}

func countableDelta(prev, next []Countable) ([]Countable, []Countable) {
	var (
		add []Countable
		del []Countable
	)

	seenInPrev := map[string]Countable{}

	for _, prevItem := range prev {
		seenInPrev[string(prevItem.Data)] = prevItem
	}

	for _, nextItem := range next {
		prev, ok := seenInPrev[string(nextItem.Data)]
		if ok && prev.TermFrequency == nextItem.TermFrequency {
			// we have an identical overlap, delete from old list
			delete(seenInPrev, string(nextItem.Data))
			// don't add to new list
			continue
		}

		add = append(add, nextItem)
	}

	// anything that's now left on the seenInPrev map must be deleted because
	// it either
	// - is no longer present
	// - is still present, but with updated values
	for _, prevItem := range prev {
		if _, ok := seenInPrev[string(prevItem.Data)]; ok {
			del = append(del, prevItem)
		}
	}

	return add, del
}

func listsIdentical(a []Countable, b []Countable) bool {
	if len(a) != len(b) {
		// can't possibly be identical if they have different lengths, exit early
		return false
	}

	for i := range a {
		if !bytes.Equal(a[i].Data, b[i].Data) ||
			a[i].TermFrequency != b[i].TermFrequency {
			// return as soon as an item didn't match
			return false
		}
	}

	// we have proven in O(n) time that both lists are identical
	// while O(n) is the worst case for this check it prevents us from running a
	// considerably more expensive merge
	return true
}

type DeltaNilResults struct {
	ToDelete []NilProperty
	ToAdd    []NilProperty
}

func DeltaNil(previous, next []NilProperty) DeltaNilResults {
	out := DeltaNilResults{}

	if previous == nil {
		out.ToAdd = next
		return out
	}

	previousByProp := map[string]NilProperty{}
	for _, prevProp := range previous {
		previousByProp[prevProp.Name] = prevProp
	}

	for _, nextProp := range next {
		if _, ok := previousByProp[nextProp.Name]; !ok {
			out.ToAdd = append(out.ToAdd, nextProp)
			continue
		}
		delete(previousByProp, nextProp.Name)
	}

	// extend ToDelete with props from previous missing in next
	for _, prevProp := range previous {
		if _, ok := previousByProp[prevProp.Name]; ok {
			out.ToDelete = append(out.ToDelete, prevProp)
		}
	}

	return out
}
