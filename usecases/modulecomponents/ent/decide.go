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

package ent

// DecisionKind is the kind of question a decisions module answers.
type DecisionKind string

const (
	// DecisionPredicate asks whether the instructions hold for the document.
	DecisionPredicate DecisionKind = "predicate"
	// DecisionChoice picks one of the options.
	DecisionChoice DecisionKind = "choice"
	// DecisionScore rates the document on ordered levels.
	DecisionScore DecisionKind = "score"
)

type DecisionOption struct {
	Value       string
	Description string
}

type DecisionLevel struct {
	Label       string
	Description string
}

// DecisionQuestion is one question about one text property of the objects
// a search returns.
type DecisionQuestion struct {
	// Name identifies the answer. Unique within one request.
	Name         string
	Property     string
	Instructions string
	Kind         DecisionKind
	// Options of a choice question.
	Options []DecisionOption
	// Levels of a score question, lowest first.
	Levels []DecisionLevel
}

// DecisionProbability is the probability of one option or one level, named
// by its value or label.
type DecisionProbability struct {
	Value       string
	Probability float64
}

// DecisionAnswer is the answer to one question for one document. The
// fields of the other kinds are zero.
type DecisionAnswer struct {
	Name string
	Kind DecisionKind
	// Refused is true when the model declined to answer; the other fields
	// are then zero.
	Refused bool
	// Probability that the predicate holds.
	Probability float64
	// Choice is the option with the highest probability.
	Choice string
	// Score is the probability-weighted position on the levels, 0 for the
	// first level.
	Score float64
	// Probabilities of every option or level, in the order of the question.
	Probabilities []DecisionProbability
	Confidence    float64
}
