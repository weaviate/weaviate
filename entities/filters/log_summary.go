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
	"encoding/json"
	"fmt"
	"reflect"
	"unicode/utf8"

	"github.com/weaviate/weaviate/entities/schema"
)

const (
	// logMaxArrayValues keeps a ContainsAny over thousands of values from
	// filling a log record, since a few values are enough to recognise the query.
	logMaxArrayValues = 16
	// logMaxOperands bounds a generated And/Or over many clauses.
	logMaxOperands = 64
	// logMaxStringChars bounds a single long text value.
	logMaxStringChars = 512
	// logMaxNodes bounds the whole tree once the per-node limits are applied.
	logMaxNodes = 512
)

// LogSummary is a bounded view of a filter for log records. It keeps the JSON
// shape of LocalFilter, adds each operator's name, and is only built when a
// formatter prints it.
type LogSummary struct {
	filter *LocalFilter
}

// LogSummary is safe to call on a nil filter and never mutates it.
func (f *LocalFilter) LogSummary() LogSummary {
	return LogSummary{filter: f}
}

func (s LogSummary) MarshalJSON() ([]byte, error) {
	if s.filter == nil {
		return []byte("null"), nil
	}
	return json.Marshal(struct {
		Root *clauseSummary `json:"root"`
	}{Root: summarizeTree(s.filter.Root)})
}

func (s LogSummary) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("<filter summary: %v>", err)
	}
	return string(b)
}

type clauseSummary struct {
	Operator     Operator      `json:"operator"`
	OperatorName string        `json:"operatorName"`
	On           *Path         `json:"on"`
	Value        *valueSummary `json:"value"`
	Operands     []any         `json:"operands"`
	OperandCount int           `json:"operandCount,omitempty"`
}

type valueSummary struct {
	Value      any             `json:"value"`
	Type       schema.DataType `json:"type"`
	ValueCount int             `json:"valueCount,omitempty"`
	CharCount  int             `json:"charCount,omitempty"`
}

// cutString replaces an array element longer than logMaxStringChars.
type cutString struct {
	Value     string `json:"value"`
	CharCount int    `json:"charCount"`
}

type truncatedSubtree struct {
	Truncated bool `json:"truncated"`
	NodeCount int  `json:"nodeCount"`
}

// summarizeTree admits clauses breadth-first until logMaxNodes is spent, so
// the top of a large tree survives and its deepest subtrees become counts.
func summarizeTree(root *Clause) *clauseSummary {
	if root == nil {
		return nil
	}
	type pending struct {
		src *Clause
		dst *clauseSummary
	}
	out := summarizeClause(root)
	budget := logMaxNodes - 1
	queue := []pending{{src: root, dst: out}}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for i := range p.src.Operands[:len(p.dst.Operands)] {
			child := &p.src.Operands[i]
			if budget == 0 {
				p.dst.Operands[i] = truncatedSubtree{Truncated: true, NodeCount: countNodes(child)}
				continue
			}
			budget--
			cs := summarizeClause(child)
			p.dst.Operands[i] = cs
			queue = append(queue, pending{src: child, dst: cs})
		}
	}
	return out
}

// summarizeClause fills everything but the operands, which summarizeTree
// assigns into the returned slots.
func summarizeClause(c *Clause) *clauseSummary {
	cs := &clauseSummary{Operator: c.Operator, OperatorName: c.Operator.Name(), On: c.On}
	if c.Value != nil {
		cs.Value = summarizeValue(c.Value)
	}
	if c.Operands != nil {
		kept := min(len(c.Operands), logMaxOperands)
		cs.Operands = make([]any, kept)
		if kept < len(c.Operands) {
			cs.OperandCount = len(c.Operands)
		}
	}
	return cs
}

func countNodes(c *Clause) int {
	n := 1
	for i := range c.Operands {
		n += countNodes(&c.Operands[i])
	}
	return n
}

func summarizeValue(v *Value) *valueSummary {
	vs := &valueSummary{Value: v.Value, Type: v.Type}
	rv := reflect.ValueOf(v.Value)
	switch rv.Kind() {
	case reflect.String:
		if cut, n := cutChars(rv.String()); n > 0 {
			vs.Value, vs.CharCount = cut, n
		}
	case reflect.Slice, reflect.Array:
		vs.Value, vs.ValueCount = summarizeArray(rv, v.Value)
	default:
	}
	return vs
}

// summarizeArray returns orig unchanged unless an element had to be dropped
// or cut, so an array within the limits marshals as the filter does.
func summarizeArray(rv reflect.Value, orig any) (any, int) {
	n := rv.Len()
	kept := min(n, logMaxArrayValues)
	changed := kept < n
	elems := make([]any, kept)
	for i := range kept {
		elems[i] = rv.Index(i).Interface()
		ev := reflect.ValueOf(elems[i])
		if ev.Kind() != reflect.String {
			continue
		}
		if cut, chars := cutChars(ev.String()); chars > 0 {
			elems[i] = cutString{Value: cut, CharCount: chars}
			changed = true
		}
	}
	if !changed {
		return orig, 0
	}
	if kept < n {
		return elems, n
	}
	return elems, 0
}

// cutChars returns s cut to logMaxStringChars characters and its original
// length in characters, or a zero length when s fits.
func cutChars(s string) (string, int) {
	if len(s) <= logMaxStringChars {
		return s, 0
	}
	chars := 0
	for i := range s {
		if chars == logMaxStringChars {
			return s[:i], utf8.RuneCountInString(s)
		}
		chars++
	}
	return s, 0
}
