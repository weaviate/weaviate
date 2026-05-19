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

package inverted

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

// TestResolveDocIDsAndOr_SequentialChildErrorIsSurfaced pins the regression
// where the sequential branch wrapped the parent's nil err instead of the
// child's err2, so child errors were silently dropped when GOMAXPROCS=1.
func TestResolveDocIDsAndOr_SequentialChildErrorIsSurfaced(t *testing.T) {
	class := &models.Class{Class: "TestClass"}
	leftChild, err := newPropValuePair(class)
	require.NoError(t, err)
	leftChild.operator = filters.OperatorEqual

	rightChild, err := newPropValuePair(class)
	require.NoError(t, err)
	rightChild.operator = filters.OperatorEqual

	parent, err := newPropValuePair(class)
	require.NoError(t, err)
	parent.operator = filters.OperatorAnd
	parent.children = []*propValuePair{leftChild, rightChild}

	s := &Searcher{logger: logrus.New()}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = concurrency.CtxWithBudget(ctx, 1)

	result, err := parent.resolveDocIDsAndOr(ctx, s)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled, "child cancellation error must propagate through the sequential branch")
}
