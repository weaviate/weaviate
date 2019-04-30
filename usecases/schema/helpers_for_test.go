/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package schema

import "github.com/creativesoftwarefdn/weaviate/contextionary"

type fakeC11yProvider struct {
}

func (f *fakeC11yProvider) GetContextionary() contextionary.Contextionary {
	return &fakeC11y{}
}

type fakeC11y struct {
}

func (f *fakeC11y) GetNumberOfItems() int {
	panic("not implemented")
}

func (f *fakeC11y) GetVectorLength() int {
	panic("not implemented")
}

func (f *fakeC11y) WordToItemIndex(word string) contextionary.ItemIndex {
	// only used to check if its present, so any non-negative value is fine for this fake
	return contextionary.ItemIndex(4)
}

func (f *fakeC11y) ItemIndexToWord(item contextionary.ItemIndex) (string, error) {
	panic("not implemented")
}

func (f *fakeC11y) GetVectorForItemIndex(item contextionary.ItemIndex) (*contextionary.Vector, error) {
	panic("not implemented")
}

func (f *fakeC11y) GetDistance(a contextionary.ItemIndex, b contextionary.ItemIndex) (float32, error) {
	panic("not implemented")
}

func (f *fakeC11y) GetNnsByItem(item contextionary.ItemIndex, n int, k int) ([]contextionary.ItemIndex, []float32, error) {
	panic("not implemented")
}

func (f *fakeC11y) GetNnsByVector(vector contextionary.Vector, n int, k int) ([]contextionary.ItemIndex, []float32, error) {
	panic("not implemented")
}

func (f *fakeC11y) SafeGetSimilarWords(word string, n int, k int) ([]string, []float32) {
	panic("not implemented")
}

func (f *fakeC11y) SafeGetSimilarWordsWithCertainty(word string, certainty float32) []string {
	panic("not implemented")
}
