//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

// An error the repo returns has to stay classifiable with errors.Is after the
// manager wrapped it. The REST layer maps sentinels raised inside the DB onto
// their own status, and a chain flattened with %v leaves it nothing to match
// on, so every one of them renders as a 500.
//
// Every entry point that funnels a repo error is covered: each has its own
// funnel, and a fix to one says nothing about the others.
func TestRepoErrStaysClassifiable(t *testing.T) {
	sentinel := errors.New("shard refused")
	repoErr := fmt.Errorf("local shard %q: %w", "s1", sentinel)

	const class = "Zoo"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	refID := strfmt.UUID("a0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "admin"}

	cases := []struct {
		name    string
		arrange func(repo *fakeVectorRepo, mods *fakeModulesProvider)
		call    func(m *Manager, b *BatchManager) error
	}{
		{
			name: "get object of a class",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.GetObject(context.Background(), principal, class, id,
					additional.Properties{}, nil, "")
				return err
			},
		},
		{
			name: "get object without a class",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("ObjectByID", id, mock.Anything, mock.Anything).
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.GetObject(context.Background(), principal, "", id,
					additional.Properties{}, nil, "")
				return err
			},
		},
		{
			name: "list objects",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything,
					mock.Anything, mock.Anything).Return([]search.Result{}, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.GetObjects(context.Background(), principal,
					nil, nil, nil, nil, nil, additional.Properties{}, "")
				return err
			},
		},
		{
			name: "head object",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("Exists", class, id).Return(false, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.HeadObject(context.Background(), principal, class, id, nil, "")
				return err
			},
		},
		{
			name: "add object fails its existence check",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("Exists", class, id).Return(false, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.AddObject(context.Background(), principal,
					&models.Object{Class: class, ID: id}, nil)
				return err
			},
		},
		{
			name: "delete object of a class",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("DeleteObject", class, id, mock.Anything).Return(repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				return m.DeleteObject(context.Background(), principal, class, id, nil, "")
			},
		},
		{
			name: "delete object without a class",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("ObjectByID", id, mock.Anything, mock.Anything).
					Return(&search.Result{ID: id, ClassName: class}, nil).Once()
				repo.On("DeleteObject", class, id, mock.Anything).Return(repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				return m.DeleteObject(context.Background(), principal, "", id, nil, "")
			},
		},
		{
			name: "batch add objects",
			arrange: func(repo *fakeVectorRepo, mods *fakeModulesProvider) {
				mods.On("BatchUpdateVector").Return(nil, nil)
				repo.On("BatchPutObjects", mock.Anything).Return(repoErr).Once()
			},
			call: func(_ *Manager, b *BatchManager) error {
				_, err := b.AddObjects(context.Background(), principal,
					[]*models.Object{{Class: class, ID: id}}, []*string{}, nil)
				return err
			},
		},
		{
			name: "batch add references",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
				repo.On("AddBatchReferences", mock.Anything).Return(repoErr).Once()
			},
			call: func(_ *Manager, b *BatchManager) error {
				_, err := b.AddReferences(context.Background(), principal,
					[]*models.BatchReference{{
						From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
						To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
					}}, nil)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, b, repo, mods, _ := newNSManagers(t, zooAnimalNSSchema(false), false)
			tc.arrange(repo, mods)

			err := tc.call(m, b)
			require.Error(t, err)
			require.ErrorIs(t, err, sentinel)
			repo.AssertExpectations(t)
		})
	}
}

// The repo types a failed shard resolution as user input, which the handler
// renders as 422. Reaching it means looking through the internal error the
// manager wraps the read in.
func TestRepoUserInputErrStaysClassifiable(t *testing.T) {
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	m, _, repo, _, _ := newNSManagers(t, zooAnimalNSSchema(false), false)
	repo.On("Object", "Zoo", id, mock.Anything, mock.Anything, "").
		Return(nil, NewErrInvalidUserInput("determine shard: %v", errors.New("no shard"))).Once()

	_, err := m.GetObject(context.Background(), &models.Principal{}, "Zoo", id,
		additional.Properties{}, nil, "")
	require.ErrorAs(t, err, &ErrInvalidUserInput{})
}

// A format with several %w verbs produces an error that unwraps to a slice,
// which errors.Unwrap reports as nil — so the constructor has to keep the
// formatted error itself to leave either cause reachable.
func TestNewErrInternalKeepsEveryCause(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")

	err := NewErrInternal("two causes: %w and %w", first, second)

	require.ErrorIs(t, err, first)
	require.ErrorIs(t, err, second)
	require.Equal(t, "two causes: first and second", err.Error())
}
