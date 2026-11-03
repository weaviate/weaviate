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

package filters

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func leaf(op Operator, prop string, value any, dt schema.DataType) Clause {
	return Clause{
		Operator: op,
		On:       &Path{Class: "C", Property: schema.PropertyName(prop)},
		Value:    &Value{Value: value, Type: dt},
	}
}

func ints(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func manyLeaves(n int) []Clause {
	out := make([]Clause, n)
	for i := range out {
		out[i] = leaf(OperatorEqual, "p", i, schema.DataTypeInt)
	}
	return out
}

// notChain wraps one leaf in n-1 Not nodes, n clauses in all.
func notChain(n int) Clause {
	c := leaf(OperatorEqual, "p", 1, schema.DataTypeInt)
	for range n - 1 {
		c = Clause{Operator: OperatorNot, Operands: []Clause{c}}
	}
	return c
}

// fullTree has the given number of levels below the root, each node with
// width operands.
func fullTree(width, levels int) Clause {
	if levels == 0 {
		return leaf(OperatorEqual, "p", 1, schema.DataTypeInt)
	}
	ops := make([]Clause, width)
	for i := range ops {
		ops[i] = fullTree(width, levels-1)
	}
	return Clause{Operator: OperatorOr, Operands: ops}
}

func summaryRoot(t *testing.T, f *LocalFilter) map[string]any {
	t.Helper()
	b, err := json.Marshal(f.LogSummary())
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(b, &out))
	root, _ := out["root"].(map[string]any)
	return root
}

func TestLogSummaryKeepsFilterWithinLimits(t *testing.T) {
	lat, lon := float32(52.5), float32(13.4)
	tests := map[string]*LocalFilter{
		"nil filter": nil,
		"nil root":   {},
		"int":        {Root: ptr(leaf(OperatorEqual, "p", 3, schema.DataTypeInt))},
		"string at char limit, multi-byte": {Root: ptr(leaf(OperatorLike, "p",
			strings.Repeat("ż", logMaxStringChars), schema.DataTypeText))},
		"array at value limit": {Root: ptr(leaf(ContainsAny, "p", ints(logMaxArrayValues), schema.DataTypeInt))},
		"string array at both limits": {Root: ptr(leaf(ContainsAll, "p",
			strings.Split(strings.TrimSuffix(strings.Repeat(strings.Repeat("a", logMaxStringChars)+",", logMaxArrayValues), ","), ","),
			schema.DataTypeText))},
		"nil array": {Root: ptr(leaf(ContainsAny, "p", []string(nil), schema.DataTypeText))},
		"geo range": {Root: ptr(leaf(OperatorWithinGeoRange, "p",
			GeoRange{GeoCoordinates: &models.GeoCoordinates{Latitude: &lat, Longitude: &lon}, Distance: 100},
			schema.DataTypeGeoCoordinates))},
		"operands at limit": {Root: &Clause{Operator: OperatorOr, Operands: manyLeaves(logMaxOperands)}},
		"empty operands":    {Root: &Clause{Operator: OperatorAnd, Operands: []Clause{}}},
		"nodes at limit":    {Root: ptr(notChain(logMaxNodes))},
		"nested logic, nested and reference paths": {Root: &Clause{
			Operator: OperatorAnd,
			Operands: []Clause{
				{Operator: OperatorNot, Operands: []Clause{leaf(OperatorEqual, "cars.make", "VW", schema.DataTypeText)}},
				{Operator: OperatorOr, Operands: []Clause{
					leaf(OperatorIsNull, "p", true, schema.DataTypeBoolean),
					{
						Operator: OperatorGreaterThan,
						On:       &Path{Class: "C", Property: "ref", Child: &Path{Class: "D", Property: "ref2", Child: &Path{Class: "E", Property: "n"}}},
						Value:    &Value{Value: 2.5, Type: schema.DataTypeNumber},
					},
				}},
			},
		}},
	}
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(f)
			require.NoError(t, err)
			var want map[string]any
			require.NoError(t, json.Unmarshal(raw, &want))
			addOperatorNames(want["root"])

			got, err := json.Marshal(f.LogSummary())
			require.NoError(t, err)
			var gotDecoded map[string]any
			require.NoError(t, json.Unmarshal(got, &gotDecoded))
			assert.Equal(t, want, gotDecoded)
			assert.Equal(t, string(got), f.LogSummary().String())
		})
	}
}

// addOperatorNames adds to a marshalled filter the one field LogSummary adds.
func addOperatorNames(node any) {
	clause, ok := node.(map[string]any)
	if !ok {
		return
	}
	clause["operatorName"] = Operator(clause["operator"].(float64)).Name()
	ops, _ := clause["operands"].([]any)
	for _, op := range ops {
		addOperatorNames(op)
	}
}

func TestLogSummaryTrimsOverLimits(t *testing.T) {
	long := strings.Repeat("ż", logMaxStringChars+1)
	cut := strings.Repeat("ż", logMaxStringChars)
	tests := map[string]struct {
		filter *LocalFilter
		check  func(t *testing.T, root map[string]any)
	}{
		"array one over value limit": {
			filter: &LocalFilter{Root: ptr(leaf(ContainsAny, "p", ints(logMaxArrayValues+1), schema.DataTypeInt))},
			check: func(t *testing.T, root map[string]any) {
				v := root["value"].(map[string]any)
				assert.Len(t, v["value"], logMaxArrayValues)
				assert.EqualValues(t, logMaxArrayValues+1, v["valueCount"])
				assert.Equal(t, "int", v["type"])
			},
		},
		"100k values, trimmed whatever the operator": {
			filter: &LocalFilter{Root: ptr(leaf(OperatorEqual, "p", ints(100_000), schema.DataTypeInt))},
			check: func(t *testing.T, root map[string]any) {
				v := root["value"].(map[string]any)
				assert.Equal(t, []any{0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0}, v["value"])
				assert.EqualValues(t, 100_000, v["valueCount"])
			},
		},
		"string one over char limit, cut on a character boundary": {
			filter: &LocalFilter{Root: ptr(leaf(OperatorEqual, "p", long, schema.DataTypeText))},
			check: func(t *testing.T, root map[string]any) {
				v := root["value"].(map[string]any)
				assert.Equal(t, cut, v["value"])
				assert.True(t, utf8.ValidString(v["value"].(string)))
				assert.EqualValues(t, logMaxStringChars+1, v["charCount"])
			},
		},
		"array element one over char limit": {
			filter: &LocalFilter{Root: ptr(leaf(ContainsAny, "p", []string{"a", long}, schema.DataTypeText))},
			check: func(t *testing.T, root map[string]any) {
				v := root["value"].(map[string]any)
				assert.Equal(t, []any{"a", map[string]any{"value": cut, "charCount": float64(logMaxStringChars + 1)}}, v["value"])
				assert.NotContains(t, v, "valueCount")
			},
		},
		"operands one over limit": {
			filter: &LocalFilter{Root: &Clause{Operator: OperatorOr, Operands: manyLeaves(logMaxOperands + 1)}},
			check: func(t *testing.T, root map[string]any) {
				assert.Len(t, root["operands"], logMaxOperands)
				assert.EqualValues(t, logMaxOperands+1, root["operandCount"])
			},
		},
		"nodes one over limit": {
			filter: &LocalFilter{Root: ptr(notChain(logMaxNodes + 1))},
			check: func(t *testing.T, root map[string]any) {
				node := root
				for range logMaxNodes - 1 {
					node = node["operands"].([]any)[0].(map[string]any)
				}
				assert.Equal(t, []any{map[string]any{"truncated": true, "nodeCount": 1.0}}, node["operands"])
			},
		},
		"deep and wide tree hits node budget": {
			filter: &LocalFilter{Root: ptr(fullTree(4, 6))},
			check: func(t *testing.T, root map[string]any) {
				admitted, inMarkers, markers := walkSummary(root)
				assert.Equal(t, logMaxNodes, admitted)
				assert.Equal(t, countNodes(ptr(fullTree(4, 6))), admitted+inMarkers)
				// levels 0-4 take 341 nodes and level 5 the other 171, so 853 level-5
				// nodes and the 4 operands of each admitted one become markers
				assert.Equal(t, 1024-171+171*4, markers)
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tt.check(t, summaryRoot(t, tt.filter))
		})
	}
}

// walkSummary counts admitted clauses, the clauses behind truncation markers
// and the markers themselves.
func walkSummary(node map[string]any) (admitted, inMarkers, markers int) {
	if node["truncated"] == true {
		return 0, int(node["nodeCount"].(float64)), 1
	}
	admitted = 1
	ops, _ := node["operands"].([]any)
	for _, op := range ops {
		a, i, m := walkSummary(op.(map[string]any))
		admitted, inMarkers, markers = admitted+a, inMarkers+i, markers+m
	}
	return admitted, inMarkers, markers
}

func TestLogSummaryTextFormatter(t *testing.T) {
	var buf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&buf)
	logger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	f := &LocalFilter{Root: ptr(leaf(ContainsAny, "p", ints(100_000), schema.DataTypeInt))}
	logger.WithField("filters", f.LogSummary()).Info("x")

	assert.Contains(t, buf.String(), `\"valueCount\":100000`)
	assert.NotContains(t, buf.String(), "0x")
	assert.Less(t, buf.Len(), 512)
}

// Shards of one query log the same filter concurrently.
func TestLogSummaryDoesNotMutateFilter(t *testing.T) {
	f := &LocalFilter{Root: &Clause{Operator: OperatorAnd, Operands: append(manyLeaves(logMaxOperands+1),
		leaf(ContainsAny, "p", ints(100), schema.DataTypeInt),
		leaf(OperatorEqual, "p", strings.Repeat("x", 2*logMaxStringChars), schema.DataTypeText),
		notChain(logMaxNodes),
	)}}
	before, err := json.Marshal(f)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = f.LogSummary().String()
		}()
	}
	wg.Wait()

	after, err := json.Marshal(f)
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after))
}

func ptr(c Clause) *Clause { return &c }
