package common_filters

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"testing"
)

// Basic test on filter
func TestExtractFilterToplevelField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorEqual,
		On: &Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("intField"),
		},
		Value: &Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := `{ FakeGet(where: { path: ["SomeAction", "intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractFilterNestedField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorEqual,
		On: &Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("hasAction"),
			Child: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("intField"),
			},
		},
		Value: &Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := `{ FakeGet(where: { path: ["SomeAction", "HasAction", "SomeAction", "intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractOperand(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorAnd,
		Operands: []Clause{Clause{
			Operator: OperatorEqual,
			On: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("intField"),
			},
			Value: &Value{
				Value: 42,
				Type:  schema.DataTypeInt,
			},
		},
			Clause{
				Operator: OperatorEqual,
				On: &Path{
					Class:    schema.AssertValidClassName("SomeAction"),
					Property: schema.AssertValidPropertyName("hasAction"),
					Child: &Path{
						Class:    schema.AssertValidClassName("SomeAction"),
						Property: schema.AssertValidPropertyName("intField"),
					},
				},
				Value: &Value{
					Value: 4242,
					Type:  schema.DataTypeInt,
				},
			},
		}}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := `{ FakeGet(where: { operator: And, operands: [
      { operator: Equal, valueInt: 42,   path: ["SomeAction", "intField"]},
      { operator: Equal, valueInt: 4242, path: ["SomeAction", "HasAction", "SomeAction", "intField"] }
    ]}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractCompareOpFailsIfOperandPresent(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	query := `{ FakeGet(where: { operator: Equal, operands: []}) }`
	resolver.AssertFailToResolve(t, query)
}

func TestExtractOperandFailsIfPathPresent(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	query := `{ FakeGet(where: { path:["should", "not", "be", "present"], operator: And  })}`
	resolver.AssertFailToResolve(t, query)
}
