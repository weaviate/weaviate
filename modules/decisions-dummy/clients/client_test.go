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

package clients

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

func TestDecideIsAFunctionOfTheText(t *testing.T) {
	questions := []ent.DecisionQuestion{
		{Name: "angry", Kind: ent.DecisionPredicate, Property: "text", Instructions: "customer angry"},
		{Name: "team", Kind: ent.DecisionChoice, Property: "text", Instructions: "which team", Options: []ent.DecisionOption{{Value: "billing"}, {Value: "other"}}},
		{Name: "length", Kind: ent.DecisionScore, Property: "text", Instructions: "how long", Levels: []ent.DecisionLevel{{Label: "short"}, {Label: "medium"}, {Label: "long"}}},
		{Name: "no", Kind: ent.DecisionPredicate, Property: "text", Instructions: RefuseInstructions},
	}

	out, err := New().Decide(context.Background(), questions, []string{"the customer is angry about billing", "hi"}, nil)

	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, ent.DecisionAnswer{Name: "angry", Kind: ent.DecisionPredicate, Probability: 1}, out[0][0])
	assert.Equal(t, ent.DecisionAnswer{Name: "angry", Kind: ent.DecisionPredicate, Probability: 0}, out[1][0])
	assert.Equal(t, "billing", out[0][1].Choice)
	assert.Equal(t, []ent.DecisionProbability{{Value: "billing", Probability: 1}, {Value: "other"}}, out[0][1].Probabilities)
	assert.Equal(t, "billing", out[1][1].Choice, "no option in the text: the first one")
	assert.Equal(t, float64(len("the customer is angry about billing")%3), out[0][2].Score)
	assert.Equal(t, float64(2), out[1][2].Score)
	assert.Equal(t, []ent.DecisionProbability{{Value: "short"}, {Value: "medium"}, {Value: "long", Probability: 1}}, out[1][2].Probabilities)
	assert.True(t, out[0][3].Refused)
}
