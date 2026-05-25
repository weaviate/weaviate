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

package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestExtractFilters_NamespaceStitching covers the new-style FilterTarget
// recursion in extractPathNew. For SingleTarget the linked class comes
// from Property.DataType which is already qualified upstream by
// QualifyPropertyDataTypes (so the parser uses it as-is). For MultiTarget
// the TargetCollection is caller-supplied (short for namespaced callers,
// prefix-validated) and must be qualified against the source's namespace.
func TestExtractFilters_NamespaceStitching(t *testing.T) {
	// Self-contained class graph mirroring what a namespace-enabled cluster
	// persists: identifiers AND ref DataType entries qualified.
	classGetter := func(name string) (*models.Class, error) {
		classes := map[string]*models.Class{
			"customer1:Zoo": {
				Class: "customer1:Zoo",
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
					{Name: "hasAnimals", DataType: []string{"customer1:Animal"}},
				},
			},
			"customer1:Animal": {
				Class: "customer1:Animal",
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
				},
			},
			"Zoo": {
				Class: "Zoo",
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
					{Name: "hasAnimals", DataType: []string{"Animal"}},
				},
			},
			"Animal": {
				Class: "Animal",
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
				},
			},
		}
		if c, ok := classes[name]; ok {
			return c, nil
		}
		return nil, nil
	}

	t.Run("SingleTarget stitches parent namespace onto short DataType", func(t *testing.T) {
		filter := &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_SingleTarget{
					SingleTarget: &pb.FilterReferenceSingleTarget{
						On: "hasAnimals",
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "lion"},
		}
		clause, err := ExtractFilters(filter, classGetter, "customer1:Zoo", "", true,
			&models.Principal{Username: "u", Namespace: "customer1"})
		require.NoError(t, err)
		require.NotNil(t, clause.On)
		// Top of the path = source class (qualified). Child = the linked
		// Animal class which must be qualified to customer1:Animal.
		assert.Equal(t, "customer1:Zoo", clause.On.Class.String())
		require.NotNil(t, clause.On.Child)
		assert.Equal(t, "customer1:Animal", clause.On.Child.Class.String(),
			"linked class must qualify via parent's namespace")
	})

	t.Run("MultiTarget rejects foreign-namespace prefix from namespaced caller", func(t *testing.T) {
		filter := &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_MultiTarget{
					MultiTarget: &pb.FilterReferenceMultiTarget{
						On:               "hasAnimals",
						TargetCollection: "customer2:Animal",
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "lion"},
		}
		_, err := ExtractFilters(filter, classGetter, "customer1:Zoo", "", true,
			&models.Principal{Username: "u", Namespace: "customer1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	t.Run("non-namespace cluster passes through unchanged", func(t *testing.T) {
		filter := &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_SingleTarget{
					SingleTarget: &pb.FilterReferenceSingleTarget{
						On: "hasAnimals",
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "lion"},
		}
		clause, err := ExtractFilters(filter, classGetter, "Zoo", "", false,
			&models.Principal{Username: "admin"})
		require.NoError(t, err)
		require.NotNil(t, clause.On)
		assert.Equal(t, "Zoo", clause.On.Class.String())
		require.NotNil(t, clause.On.Child)
		assert.Equal(t, "Animal", clause.On.Child.Class.String(),
			"non-namespace clusters: linked class stays short")
	})

	t.Run("MultiTarget global principal: own-namespace prefix does not double-qualify", func(t *testing.T) {
		// Regression: admins on NS-enabled clusters may legitimately spell
		// out their own namespace prefix in TargetCollection. The old code
		// re-prepended parentNS, producing "customer1:customer1:Animal" and
		// missing the schema lookup. The fix routes through QualifyRefTarget
		// which strips the prefix before re-qualifying.
		filter := &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_MultiTarget{
					MultiTarget: &pb.FilterReferenceMultiTarget{
						On:               "hasAnimals",
						TargetCollection: "customer1:Animal",
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "lion"},
		}
		clause, err := ExtractFilters(filter, classGetter, "customer1:Zoo", "", true,
			&models.Principal{Username: "admin"})
		require.NoError(t, err)
		require.NotNil(t, clause.On)
		require.NotNil(t, clause.On.Child)
		assert.Equal(t, "customer1:Animal", clause.On.Child.Class.String(),
			"admin's own-namespace prefix must not double-qualify")
	})

	t.Run("MultiTarget global principal: short TargetCollection qualifies via parent NS", func(t *testing.T) {
		filter := &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_MultiTarget{
					MultiTarget: &pb.FilterReferenceMultiTarget{
						On:               "hasAnimals",
						TargetCollection: "Animal",
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "lion"},
		}
		clause, err := ExtractFilters(filter, classGetter, "customer1:Zoo", "", true,
			&models.Principal{Username: "admin"})
		require.NoError(t, err)
		require.NotNil(t, clause.On)
		require.NotNil(t, clause.On.Child)
		assert.Equal(t, "customer1:Animal", clause.On.Child.Class.String(),
			"short TargetCollection must qualify via source-class namespace")
	})

	t.Run("MultiTarget global principal: foreign-namespace prefix is rejected", func(t *testing.T) {
		// Refs can't cross namespaces. The old code silently accepted a
		// foreign prefix from a global principal and built
		// "customer1:customer2:Animal"; QualifyRefTarget now rejects it.
		filter := &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_MultiTarget{
					MultiTarget: &pb.FilterReferenceMultiTarget{
						On:               "hasAnimals",
						TargetCollection: "customer2:Animal",
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "lion"},
		}
		_, err := ExtractFilters(filter, classGetter, "customer1:Zoo", "", true,
			&models.Principal{Username: "admin"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	t.Run("AND/OR clauses thread principal through to nested ExtractFilters", func(t *testing.T) {
		// Regression guard: the recursion of ExtractFilters into nested
		// filterIn.Filters must propagate principal so cross-namespace deny
		// fires inside boolean compositions too.
		filter := &pb.Filters{
			Operator: pb.Filters_OPERATOR_AND,
			Filters: []*pb.Filters{
				{
					Operator: pb.Filters_OPERATOR_EQUAL,
					Target: &pb.FilterTarget{
						Target: &pb.FilterTarget_MultiTarget{
							MultiTarget: &pb.FilterReferenceMultiTarget{
								On:               "hasAnimals",
								TargetCollection: "customer2:Animal",
								Target: &pb.FilterTarget{
									Target: &pb.FilterTarget_Property{Property: "name"},
								},
							},
						},
					},
					TestValue: &pb.Filters_ValueText{ValueText: "lion"},
				},
			},
		}
		_, err := ExtractFilters(filter, classGetter, "customer1:Zoo", "", true,
			&models.Principal{Username: "u", Namespace: "customer1"})
		require.Error(t, err)
	})
}
