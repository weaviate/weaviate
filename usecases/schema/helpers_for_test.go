//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
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
func (f *fakeC11y) IsWordPresent(ctx context.Context, word string) (bool, error) {
	if word == "carrot" || word == "the" {
		return false, nil
	}
	return true, nil
}

type fakeStopwordDetector struct{}

func (f *fakeStopwordDetector) IsStopWord(ctx context.Context, word string) (bool, error) {
	return word == "the", nil
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}
