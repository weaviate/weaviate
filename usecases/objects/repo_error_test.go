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
func TestRepoErrStaysClassifiable(t *testing.T) {
	sentinel := errors.New("shard refused")
	repoErr := fmt.Errorf("local shard %q: %w", "s1", sentinel)

	const class = "MyClass"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")

	cases := []struct {
		name    string
		arrange func(repo *fakeVectorRepo)
		call    func(m *Manager) error
	}{
		{
			name: "get object of a class",
			arrange: func(repo *fakeVectorRepo) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager) error {
				_, err := m.GetObject(context.Background(), &models.Principal{}, class, id,
					additional.Properties{}, nil, "")
				return err
			},
		},
		{
			name: "get object without a class",
			arrange: func(repo *fakeVectorRepo) {
				repo.On("ObjectByID", id, mock.Anything, mock.Anything).
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager) error {
				_, err := m.GetObject(context.Background(), &models.Principal{}, "", id,
					additional.Properties{}, nil, "")
				return err
			},
		},
		{
			name: "list objects",
			arrange: func(repo *fakeVectorRepo) {
				repo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything,
					mock.Anything, mock.Anything).Return([]search.Result{}, repoErr).Once()
			},
			call: func(m *Manager) error {
				_, err := m.GetObjects(context.Background(), &models.Principal{},
					nil, nil, nil, nil, nil, additional.Properties{}, "")
				return err
			},
		},
		{
			name: "delete object of a class",
			arrange: func(repo *fakeVectorRepo) {
				repo.On("DeleteObject", class, id, mock.Anything).Return(repoErr).Once()
			},
			call: func(m *Manager) error {
				return m.DeleteObject(context.Background(), &models.Principal{}, class, id, nil, "")
			},
		},
		{
			name: "head object",
			arrange: func(repo *fakeVectorRepo) {
				repo.On("Exists", class, id).Return(false, repoErr).Once()
			},
			call: func(m *Manager) error {
				_, err := m.HeadObject(context.Background(), &models.Principal{}, class, id, nil, "")
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager, repo, _, _ := newDeleteDependency()
			tc.arrange(repo)

			err := tc.call(manager)
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
	manager, repo, _, _ := newDeleteDependency()
	repo.On("Object", "MyClass", id, mock.Anything, mock.Anything, "").
		Return(nil, NewErrInvalidUserInput("determine shard: %v", errors.New("no shard"))).Once()

	_, err := manager.GetObject(context.Background(), &models.Principal{}, "MyClass", id,
		additional.Properties{}, nil, "")
	require.ErrorAs(t, err, &ErrInvalidUserInput{})
}
