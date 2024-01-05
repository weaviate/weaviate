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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestReadConsensus(t *testing.T) {
	type test struct {
		name           string
		in             []*cluster.Transaction
		expectedResult *cluster.Transaction
		expectError    bool
		parser         parserFn
	}

	tests := []test{
		{
			name: "different (unrelated) tx type (no consensus required)",
			in: []*cluster.Transaction{
				{
					Type: AddClass,
					ID:   "id1",
				},
			},
			expectError: false,
		},
		{
			name: "single schema with content",
			in: []*cluster.Transaction{
				wrapSchemaAsRawReadTx(&State{
					ShardingState: map[string]*sharding.State{
						"Foo": {
							IndexID: "1234",
						},
					},
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "Foo",
							},
						},
					},
				}),
			},
			expectedResult: wrapSchemaAsReadTx(&State{
				ShardingState: map[string]*sharding.State{
					"Foo": {
						IndexID: "1234",
					},
				},
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class: "Foo",
						},
					},
				},
			}),
		},
		{
			name: "two identical empty schemas",
			in: []*cluster.Transaction{
				wrapSchemaAsRawReadTx(newSchema()),
				wrapSchemaAsRawReadTx(newSchema()),
			},
			expectedResult: wrapSchemaAsReadTx(newSchema()),
		},
		{
			name: "two identical filled schemas",
			in: []*cluster.Transaction{
				wrapSchemaAsRawReadTx(&State{
					ShardingState: map[string]*sharding.State{
						"Foo": {
							IndexID: "1234",
						},
					},
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "Foo",
							},
						},
					},
				}),
				wrapSchemaAsRawReadTx(&State{
					ShardingState: map[string]*sharding.State{
						"Foo": {
							IndexID: "1234",
						},
					},
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "Foo",
							},
						},
					},
				}),
			},
			expectedResult: wrapSchemaAsReadTx(&State{
				ShardingState: map[string]*sharding.State{
					"Foo": {
						IndexID: "1234",
					},
				},
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class: "Foo",
						},
					},
				},
			}),
		},
		{
			name: "3 responses with a conflict",
			in: []*cluster.Transaction{
				wrapSchemaAsRawReadTx(&State{
					ShardingState: map[string]*sharding.State{
						"Foo": {
							IndexID: "1234",
						},
					},
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "Foo",
							},
						},
					},
				}),
				wrapSchemaAsRawReadTx(&State{
					ShardingState: map[string]*sharding.State{
						"Foo": {
							IndexID: "1234",
						},
					},
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "Foo",
							},
						},
					},
				}),
				wrapSchemaAsRawReadTx(&State{
					ShardingState: map[string]*sharding.State{
						"Foo": {
							IndexID: "1234",
						},
					},
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "Foo",
								//  the other classes don't have the vector index set:
								VectorIndexType: "La-Forca-de-Bruta",
							},
						},
					},
				}),
			},
			expectError: true,
		},
		{
			name: "tx id mismatch",
			in: []*cluster.Transaction{
				{
					Type:    ReadSchema,
					Payload: json.RawMessage("null"),
					ID:      "id1",
				},
				{
					Type:    ReadSchema,
					Payload: json.RawMessage("null"),
					ID:      "id2",
				},
			},
			expectError: true,
		},
		{
			name: "invalid payload json",
			in: []*cluster.Transaction{
				{
					Type:    ReadSchema,
					Payload: json.RawMessage("----<@#()$*--"),
					ID:      "id1",
				},
			},
			expectError: true,
		},
		{
			name: "invalid payload json",
			in: []*cluster.Transaction{
				{
					Type:    ReadSchema,
					Payload: json.RawMessage("----<@#()$*--"),
					ID:      "id1",
				},
			},
			expectError: true,
		},
		{
			name: "schema parse error",
			parser: func(ctx context.Context, s *State) error {
				return fmt.Errorf("not so fast there, Mister Schema!")
			},
			in: []*cluster.Transaction{
				wrapSchemaAsRawReadTx(&State{
					ShardingState: map[string]*sharding.State{
						"Foo": {
							IndexID: "1234",
						},
					},
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "Foo",
							},
						},
					},
				}),
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := dummyParser
			if test.parser != nil {
				parser = test.parser
			}

			logger, _ := logrustest.NewNullLogger()
			out, err := newReadConsensus(parser, logger)(context.Background(), test.in)

			if test.expectError {
				require.NotNil(t, err, "must error")
			} else {
				require.Nil(t, err)
			}

			assert.Equal(t, test.expectedResult, out)
		})
	}
}

// raw = before unmarshalling the payload
func wrapSchemaAsRawReadTx(s *State) *cluster.Transaction {
	payloadJSON, _ := json.Marshal(ReadSchemaPayload{
		Schema: s,
	})
	return &cluster.Transaction{
		Type:    ReadSchema,
		Payload: json.RawMessage(payloadJSON),
		ID:      "best-tx",
	}
}

func wrapSchemaAsReadTx(s *State) *cluster.Transaction {
	return &cluster.Transaction{
		Type:    ReadSchema,
		Payload: ReadSchemaPayload{Schema: s},
		ID:      "best-tx",
	}
}

func dummyParser(ctx context.Context, schema *State) error {
	return nil
}
