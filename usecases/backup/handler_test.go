//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

type fakeSchemaManger struct{}

func (f *fakeSchemaManger) RestoreClass(context.Context, *models.Principal, *backup.ClassDescriptor,
) error {
	return nil
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

func TestFilerClasses(t *testing.T) {
	tests := []struct {
		in  []string
		xs  []string
		out []string
	}{
		{in: []string{}, xs: []string{}, out: []string{}},
		{in: []string{"a"}, xs: []string{}, out: []string{"a"}},
		{in: []string{"a"}, xs: []string{"a"}, out: []string{}},
		{in: []string{"1", "2", "3", "4"}, xs: []string{"2", "3"}, out: []string{"1", "4"}},
		{in: []string{"1", "2", "3"}, xs: []string{"1", "3"}, out: []string{"2"}},
	}
	for _, tc := range tests {
		got := filter_classes(tc.in, tc.xs)
		assert.Equal(t, tc.out, got)
	}
}
