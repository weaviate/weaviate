/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package schema

import (
	"context"

	"github.com/semi-technologies/weaviate/contextionary"
)

type fakeRepo struct {
	schema *State
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{}
}

func (f *fakeRepo) LoadSchema(context.Context) (*State, error) {
	return f.schema, nil
}

func (f *fakeRepo) SaveSchema(ctx context.Context, schema State) error {
	f.schema = &schema
	return nil
}

type fakeLocks struct{}

func newFakeLocks() *fakeLocks {
	return &fakeLocks{}
}

func (f *fakeLocks) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}

func (f *fakeLocks) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

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

// Every word in this fake c11y is present with the same position (4), except
// for the word Carrot which is not present
func (f *fakeC11y) WordToItemIndex(word string) contextionary.ItemIndex {
	if word == "carrot" || word == "the" {
		return contextionary.ItemIndex(-1)
	}
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

func (f *fakeC11y) ItemIndexToOccurrence(item contextionary.ItemIndex) (uint64, error) {
	panic("not implemented")
}

type fakeStopwordDetector struct{}

func (f *fakeStopwordDetector) IsStopWord(word string) bool {
	return word == "the"
}
