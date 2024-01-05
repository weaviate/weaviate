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

package classification

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// warning, not thread-safe for this spike

type TfIdfCalculator struct {
	size            int
	documents       []string
	documentLengths []uint
	docPointer      int
	terms           map[string][]uint16
	termIdf         map[string]float32
}

func NewTfIdfCalculator(size int) *TfIdfCalculator {
	return &TfIdfCalculator{
		size:            size,
		documents:       make([]string, size),
		documentLengths: make([]uint, size),
		terms:           make(map[string][]uint16),
		termIdf:         make(map[string]float32),
	}
}

func (c *TfIdfCalculator) AddDoc(doc string) error {
	if c.docPointer > c.size {
		return fmt.Errorf("doc size exceeded")
	}

	c.documents[c.docPointer] = doc
	c.docPointer++
	return nil
}

func (c *TfIdfCalculator) Calculate() {
	for i := range c.documents {
		c.analyzeDoc(i)
	}

	for term, frequencies := range c.terms {
		var contained uint
		for _, frequency := range frequencies {
			if frequency > 0 {
				contained++
			}
		}

		c.termIdf[term] = float32(math.Log10(float64(c.size) / float64(contained)))
	}
}

func (c *TfIdfCalculator) analyzeDoc(docIndex int) {
	terms := newSplitter().Split(c.documents[docIndex])
	for i, term := range terms {
		term = strings.ToLower(term)
		frequencies := c.getOrInitTerm(term)
		frequencies[docIndex] = frequencies[docIndex] + 1
		c.documentLengths[docIndex] = uint(i + 1)
		c.terms[term] = frequencies
	}
}

func (c *TfIdfCalculator) getOrInitTerm(term string) []uint16 {
	frequencies, ok := c.terms[term]
	if !ok {
		frequencies := make([]uint16, c.size)
		c.terms[term] = frequencies
		return frequencies
	}

	return frequencies
}

func (c *TfIdfCalculator) Get(term string, doc int) float32 {
	term = strings.ToLower(term)
	frequencies, ok := c.terms[term]
	if !ok {
		return 0
	}

	tf := float32(frequencies[doc]) / float32(c.documentLengths[doc])
	idf := c.termIdf[term]

	return tf * idf
}

func (c *TfIdfCalculator) GetAllTerms(docIndex int) []TermWithTfIdf {
	terms := newSplitter().Split(c.documents[docIndex])
	terms = c.lowerCaseAndDedup(terms)

	out := make([]TermWithTfIdf, len(terms))
	for i, term := range terms {
		out[i] = TermWithTfIdf{
			Term:  term,
			TfIdf: c.Get(term, docIndex),
		}
	}

	sort.Slice(out, func(a, b int) bool { return out[a].TfIdf > out[b].TfIdf })
	return c.withRelativeScores(out)
}

type TermWithTfIdf struct {
	Term          string
	TfIdf         float32
	RelativeScore float32
}

func (c *TfIdfCalculator) withRelativeScores(list []TermWithTfIdf) []TermWithTfIdf {
	// mean for variance
	var mean float64
	for _, t := range list {
		mean += float64(t.TfIdf)
	}
	mean = mean / float64(len(list))

	// calculate variance
	for i, t := range list {
		variance := math.Pow(float64(t.TfIdf)-mean, 2)
		if float64(t.TfIdf) < mean {
			list[i].RelativeScore = float32(-variance)
		} else {
			list[i].RelativeScore = float32(variance)
		}
	}

	return c.withNormalizedScores(list)
}

// between -1 and 1
func (c *TfIdfCalculator) withNormalizedScores(list []TermWithTfIdf) []TermWithTfIdf {
	max, min := c.maxMin(list)

	for i, curr := range list {
		score := (curr.RelativeScore - min) / (max - min)
		list[i].RelativeScore = (score - 0.5) * 2
	}

	return list
}

func (c *TfIdfCalculator) maxMin(list []TermWithTfIdf) (float32, float32) {
	max := list[0].RelativeScore
	min := list[0].RelativeScore

	for _, curr := range list {
		if curr.RelativeScore > max {
			max = curr.RelativeScore
		}
		if curr.RelativeScore < min {
			min = curr.RelativeScore
		}
	}

	return max, min
}

func (c *TfIdfCalculator) lowerCaseAndDedup(list []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, len(list))
	i := 0
	for _, term := range list {
		term = strings.ToLower(term)
		_, ok := seen[term]
		if ok {
			continue
		}

		seen[term] = struct{}{}
		out[i] = term
		i++
	}

	return out[:i]
}
