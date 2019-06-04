package vectorizer

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorizingThings(t *testing.T) {
	type testCase struct {
		name               string
		input              *models.Thing
		expectedClientCall []string
	}

	tests := []testCase{
		testCase{
			name: "empty thing",
			input: &models.Thing{
				Class: "Car",
			},
			expectedClientCall: []string{"car"},
		},
		testCase{
			name: "thing with one string prop",
			input: &models.Thing{
				Class: "Car",
				Schema: map[string]interface{}{
					"brand": "Mercedes",
				},
			},
			expectedClientCall: []string{"car", "brand mercedes"},
		},

		testCase{
			name: "thing with one non-string prop",
			input: &models.Thing{
				Class: "Car",
				Schema: map[string]interface{}{
					"power": 300,
				},
			},
			expectedClientCall: []string{"car", "power"},
		},

		testCase{
			name: "thing with a mix of props",
			input: &models.Thing{
				Class: "Car",
				Schema: map[string]interface{}{
					"brand":  "best brand",
					"power":  300,
					"review": "a very great car",
				},
			},
			expectedClientCall: []string{"car", "brand best brand",
				"power", "review a very great car"},
		},

		testCase{
			name: "with compound class and prop names",
			input: &models.Thing{
				Class: "SuperCar",
				Schema: map[string]interface{}{
					"brandOfTheCar": "best brand",
					"power":         300,
					"review":        "a very great car",
				},
			},
			expectedClientCall: []string{"super car", "brand of the car best brand",
				"power", "review a very great car"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			v := New(client)

			res, err := v.Thing(context.Background(), test.input)

			require.Nil(t, err)
			assert.Equal(t, []float32{0, 1, 2, 3}, res)
			assert.ElementsMatch(t, test.expectedClientCall, client.lastInput)

		})

	}
}

func TestVectorizingActions(t *testing.T) {
	type testCase struct {
		name               string
		input              *models.Action
		expectedClientCall []string
	}

	tests := []testCase{
		testCase{
			name: "empty thing",
			input: &models.Action{
				Class: "Flight",
			},
			expectedClientCall: []string{"flight"},
		},
		testCase{
			name: "thing with one string prop",
			input: &models.Action{
				Class: "Flight",
				Schema: map[string]interface{}{
					"brand": "Mercedes",
				},
			},
			expectedClientCall: []string{"flight", "brand mercedes"},
		},

		testCase{
			name: "thing with one non-string prop",
			input: &models.Action{
				Class: "Flight",
				Schema: map[string]interface{}{
					"length": 300,
				},
			},
			expectedClientCall: []string{"flight", "length"},
		},

		testCase{
			name: "thing with a mix of props",
			input: &models.Action{
				Class: "Flight",
				Schema: map[string]interface{}{
					"brand":  "best brand",
					"length": 300,
					"review": "a very great flight",
				},
			},
			expectedClientCall: []string{"flight", "brand best brand",
				"length", "review a very great flight"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			v := New(client)

			res, err := v.Action(context.Background(), test.input)

			require.Nil(t, err)
			assert.Equal(t, []float32{0, 1, 2, 3}, res)
			assert.ElementsMatch(t, test.expectedClientCall, client.lastInput)

		})

	}
}

type fakeClient struct {
	lastInput []string
}

func (c *fakeClient) VectorForCorpi(ctx context.Context, corpi []string) ([]float32, error) {
	c.lastInput = corpi
	return []float32{0, 1, 2, 3}, nil
}
