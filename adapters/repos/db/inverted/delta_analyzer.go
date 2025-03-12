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
	return DeltaSkipSearchable(previous, next, nil)
}

// skipDeltaSearchableProps - names of properties having searchable index of StrategyInverted
// (StrategyInverted requires complete set of items to be added or deleted (not delta),
// therefore for such properties calculating delta should be skipped. If same properties
// have other indexes (filterable / rangeable) property will be duplicated to contain
// complete items sets for searchable index and delta for remaining ones.
func DeltaSkipSearchable(previous, next []Property, skipDeltaSearchableProps []string) DeltaResults {
	out := DeltaResults{}
	previous = DedupItems(previous)
	next = DedupItems(next)

	if previous == nil {
		out.ToAdd = next
		return out
	}

	skipDeltaPropsNames := map[string]struct{}{}
	for i := range skipDeltaSearchableProps {
		skipDeltaPropsNames[skipDeltaSearchableProps[i]] = struct{}{}
	}

	previousByProp := map[string]Property{}
	for _, prevProp := range previous {
		previousByProp[prevProp.Name] = prevProp
	}

	for _, nextProp := range next {
		prevProp, ok := previousByProp[nextProp.Name]
		if !ok {
			if len(nextProp.Items) == 0 {
				// effectively nothing is added
				continue
			}

			// this prop didn't exist before so we can add all of it
			out.ToAdd = append(out.ToAdd, nextProp)
			if nextProp.Length != -1 {
				// if length supported, remove prev length
				out.ToDelete = append(out.ToDelete, Property{
					Name:               nextProp.Name,
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: nextProp.HasFilterableIndex,
					HasSearchableIndex: nextProp.HasSearchableIndex,
					HasRangeableIndex:  nextProp.HasRangeableIndex,
				})
			}
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

		if lenPrev, lenNext := len(prevProp.Items), len(nextProp.Items); lenPrev == 0 || lenNext == 0 {
			out.ToAdd = append(out.ToAdd, nextProp)
			out.ToDelete = append(out.ToDelete, prevProp)
		} else {
			_, skipDeltaSearchable := skipDeltaPropsNames[nextProp.Name]

			if skipDeltaSearchable && nextProp.HasSearchableIndex {
				// property with searchable index of StrategyInverted
				if !nextProp.HasFilterableIndex && !nextProp.HasRangeableIndex {
					// no other indexes, skip calculating delta
					out.ToAdd = append(out.ToAdd, nextProp)
					out.ToDelete = append(out.ToDelete, prevProp)
				} else {
					// other indexes present
					toAdd, toDel, cleaned := countableDelta(prevProp.Items, nextProp.Items)

					// if delta same as inputs
					if !cleaned {
						out.ToAdd = append(out.ToAdd, nextProp)
						out.ToDelete = append(out.ToDelete, prevProp)
					} else {
						// separate entries for !searchable indexes with calculated delta
						out.ToAdd = append(out.ToAdd, Property{
							Name:               nextProp.Name,
							Items:              toAdd,
							Length:             nextProp.Length,
							HasFilterableIndex: nextProp.HasFilterableIndex,
							HasSearchableIndex: false,
							HasRangeableIndex:  nextProp.HasRangeableIndex,
						})
						out.ToDelete = append(out.ToDelete, Property{
							Name:               prevProp.Name,
							Items:              toDel,
							Length:             prevProp.Length,
							HasFilterableIndex: prevProp.HasFilterableIndex,
							HasSearchableIndex: false,
							HasRangeableIndex:  prevProp.HasRangeableIndex,
						})

						// separate entries for searchable index of StrategyInverted with complete item sets
						// length/nil indexes will be handled by delta entries, therefore -1 not to be processed twice
						out.ToAdd = append(out.ToAdd, Property{
							Name:               nextProp.Name,
							Items:              nextProp.Items,
							Length:             -1,
							HasFilterableIndex: false,
							HasSearchableIndex: true,
							HasRangeableIndex:  false,
						})
						out.ToDelete = append(out.ToDelete, Property{
							Name:               prevProp.Name,
							Items:              prevProp.Items,
							Length:             -1,
							HasFilterableIndex: false,
							HasSearchableIndex: true,
							HasRangeableIndex:  false,
						})
					}
				}
			} else {
				// property of other indexes, calculate delta
				toAdd, toDel, _ := countableDelta(prevProp.Items, nextProp.Items)
				out.ToAdd = append(out.ToAdd, Property{
					Name:               nextProp.Name,
					Items:              toAdd,
					Length:             nextProp.Length,
					HasFilterableIndex: nextProp.HasFilterableIndex,
					HasSearchableIndex: nextProp.HasSearchableIndex,
					HasRangeableIndex:  nextProp.HasRangeableIndex,
				})
				out.ToDelete = append(out.ToDelete, Property{
					Name:               prevProp.Name,
					Items:              toDel,
					Length:             prevProp.Length,
					HasFilterableIndex: prevProp.HasFilterableIndex,
					HasSearchableIndex: prevProp.HasSearchableIndex,
					HasRangeableIndex:  prevProp.HasRangeableIndex,
				})
			}
		}
	}

	// extend ToDelete with props from previous missing in next
	for _, prevProp := range previous {
		if _, ok := previousByProp[prevProp.Name]; ok {
			if len(prevProp.Items) == 0 {
				// effectively nothing is removed
				continue
			}

			if prevProp.Length != -1 {
				// if length supported, add next length
				out.ToAdd = append(out.ToAdd, Property{
					Name:               prevProp.Name,
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: prevProp.HasFilterableIndex,
					HasSearchableIndex: prevProp.HasSearchableIndex,
					HasRangeableIndex:  prevProp.HasRangeableIndex,
				})
			}
			out.ToDelete = append(out.ToDelete, prevProp)
		}
	}

	return out
}

func countableDelta(prev, next []Countable) ([]Countable, []Countable, bool) {
	add := []Countable{}
	del := []Countable{}

	seenInPrev := map[string]Countable{}
	cleaned := false

	for _, prevItem := range prev {
		seenInPrev[string(prevItem.Data)] = prevItem
	}

	for _, nextItem := range next {
		prev, ok := seenInPrev[string(nextItem.Data)]
		if ok && prev.TermFrequency == nextItem.TermFrequency {
			cleaned = true
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

	return add, del, cleaned
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
