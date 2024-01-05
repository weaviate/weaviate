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
	"sort"
	"testing"

	testlog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestSchemaRepair(t *testing.T) {
	type (
		properties = []*models.Property
		classes    = []*models.Class
		testCase   struct {
			name          string
			originalLocal State
			remote        State
			tenants       map[string][]string
		}
	)

	var (
		ctx              = context.Background()
		logger, _        = testlog.NewNullLogger()
		clusterState     = &fakeClusterState{hosts: []string{"some.host"}}
		txManager        = cluster.NewTxManager(&fakeBroadcaster{}, &fakeTxPersistence{}, logger)
		newShardingState = func(id string, tenants ...string) *sharding.State {
			ss := &sharding.State{
				IndexID: id,
				Physical: func() map[string]sharding.Physical {
					m := map[string]sharding.Physical{}
					for _, tenant := range tenants {
						m[tenant] = sharding.Physical{
							Name:           tenant,
							BelongsToNodes: []string{clusterState.LocalName()},
						}
					}
					return m
				}(),
				Virtual:             make([]sharding.Virtual, 0),
				PartitioningEnabled: false,
			}
			ss.SetLocalName(clusterState.LocalName())
			return ss
		}
		newClass = func(name string, props properties, mtEnabled bool) *models.Class {
			return &models.Class{
				Class:              name,
				Properties:         props,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: mtEnabled},
				ShardingConfig:     sharding.Config{},
				ReplicationConfig:  &models.ReplicationConfig{},
			}
		}
		newProp = func(name, dt string) *models.Property {
			return &models.Property{
				Name:     name,
				DataType: []string{dt},
			}
		}
	)

	tests := []testCase{
		{
			name: "one class repaired locally",
			originalLocal: State{
				ObjectSchema: &models.Schema{Classes: classes{
					newClass("Class1", properties{newProp("textProp", "text")}, false),
				}},
			},
			remote: State{
				ObjectSchema: &models.Schema{Classes: classes{
					newClass("Class1", properties{newProp("textProp", "text")}, false),
					newClass("Class2", properties{newProp("intProp", "int")}, false),
				}},
			},
		},
		{
			name: "one class's properties repaired locally",
			originalLocal: State{
				ObjectSchema: &models.Schema{Classes: classes{
					newClass("Class1", properties{newProp("intProp", "int")}, false),
					newClass("Class2", properties{newProp("intProp", "int")}, false),
				}},
			},
			remote: State{
				ObjectSchema: &models.Schema{Classes: classes{
					newClass("Class1", properties{newProp("textProp", "text"), newProp("intProp", "int")}, false),
					newClass("Class2", properties{newProp("intProp", "int")}, false),
				}},
			},
		},
		{
			name: "one class's tenants repaired locally",
			originalLocal: State{
				ObjectSchema: &models.Schema{Classes: classes{
					newClass("Class1", properties{newProp("textProp", "text")}, true),
				}},
			},
			remote: State{
				ObjectSchema: &models.Schema{Classes: classes{
					newClass("Class1", properties{newProp("textProp", "text")}, true),
				}},
			},
			tenants: map[string][]string{
				"Class1": {"tenant1", "tenant2"},
			},
		},
	}

	t.Run("init testcase sharding states", func(t *testing.T) {
		for i := range tests {
			tests[i].originalLocal.ShardingState = map[string]*sharding.State{}
			for _, class := range tests[i].originalLocal.ObjectSchema.Classes {
				ss := newShardingState(class.Class)
				tests[i].originalLocal.ShardingState[class.Class] = ss
			}
			tests[i].remote.ShardingState = map[string]*sharding.State{}
			for _, class := range tests[i].remote.ObjectSchema.Classes {
				tenants := tests[i].tenants[class.Class]
				ss := newShardingState(class.Class, tenants...)
				tests[i].remote.ShardingState[class.Class] = ss
			}
		}
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := &Manager{
				logger:       logger,
				clusterState: clusterState,
				cluster:      txManager,
				repo:         &fakeRepo{schema: test.originalLocal},
				schemaCache:  schemaCache{State: test.originalLocal},
			}
			require.Nil(t, m.repo.Save(ctx, test.originalLocal))

			t.Run("run repair", func(t *testing.T) {
				err := m.repairSchema(ctx, &test.remote)
				assert.Nil(t, err, "expected nil err, got: %v", err)
			})

			t.Run("assert local and remote are in sync", func(t *testing.T) {
				t.Run("compare classes", func(t *testing.T) {
					expected := test.remote.ObjectSchema.Classes
					received := m.schemaCache.State.ObjectSchema.Classes
					// Sort the classes and their properties for easier comparison
					sortSchemaClasses(expected)
					sortSchemaClasses(received)
					assert.ElementsMatch(t, expected, received)
				})

				t.Run("compare sharding states", func(t *testing.T) {
					for id, ss := range m.ShardingState {
						expectedSS, found := test.remote.ShardingState[id]
						assert.True(t, found)
						assert.EqualValues(t, expectedSS, ss)
					}
				})
			})
		})
	}
}

func sortSchemaClasses(classes []*models.Class) {
	sort.Slice(classes, func(i, j int) bool {
		return classes[i].Class > classes[j].Class
	})
	for _, class := range classes {
		sort.Slice(class.Properties, func(i, j int) bool {
			return class.Properties[i].Name > class.Properties[j].Name
		})
	}
}
