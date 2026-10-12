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

// Package decisions runs the decide query argument end to end against the
// decisions-dummy module, whose answers are a function of the document
// text: a predicate's probability is the share of the instruction words in
// the text, a choice is the first option named in the text, a score is the
// length of the text modulo the number of levels, and the instructions
// "refuse" refuse.
package decisions

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	grpchelper "github.com/weaviate/weaviate/test/helper/grpc"
)

const (
	ticketsClass = "DecideTickets"
	angryText    = "the customer is angry about billing"
	calmText     = "all good thanks"
)

func TestDecide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("ENABLE_MODULES", "decisions-dummy,reranker-dummy").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	predicate := func(name, property, instructions string) *pb.DecisionQuestion {
		return &pb.DecisionQuestion{
			Name: name, Property: property, Instructions: instructions,
			Kind: &pb.DecisionQuestion_Predicate{Predicate: &pb.DecisionPredicate{}},
		}
	}
	choice := func(name, property string, options ...string) *pb.DecisionQuestion {
		question := &pb.DecisionQuestion{
			Name: name, Property: property, Instructions: "which",
			Kind: &pb.DecisionQuestion_Choice{Choice: &pb.DecisionChoice{}},
		}
		for _, option := range options {
			question.GetChoice().Options = append(question.GetChoice().Options, &pb.DecisionOption{Value: option})
		}
		return question
	}
	score := func(name, property string, levels ...string) *pb.DecisionQuestion {
		question := &pb.DecisionQuestion{
			Name: name, Property: property, Instructions: "how much",
			Kind: &pb.DecisionQuestion_Score{Score: &pb.DecisionScore{}},
		}
		for _, level := range levels {
			question.GetScore().Levels = append(question.GetScore().Levels, &pb.DecisionLevel{Label: level})
		}
		return question
	}
	// byText maps the text of every result to its answers by question name.
	byText := func(t *testing.T, reply *pb.SearchReply) map[string]map[string]*pb.DecisionAnswer {
		t.Helper()
		out := map[string]map[string]*pb.DecisionAnswer{}
		for _, result := range reply.Results {
			text := result.Properties.NonRefProps.Fields["text"].GetTextValue()
			require.NotNil(t, result.Decisions, "result %q has no decisions", text)
			answers := map[string]*pb.DecisionAnswer{}
			for _, answer := range result.Decisions.Answers {
				answers[answer.Name] = answer
			}
			out[text] = answers
		}
		return out
	}

	createClass := func(t *testing.T, name string, moduleConfig map[string]any) {
		t.Helper()
		helper.DeleteClass(t, name)
		helper.CreateClass(t, &models.Class{
			Class:      name,
			Vectorizer: "none",
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
				{Name: "count", DataType: []string{"int"}},
			},
			ModuleConfig: moduleConfig,
		})
		helper.CreateObjectsBatch(t, []*models.Object{
			{Class: name, ID: strfmt.UUID(uuid.NewString()), Properties: map[string]any{"text": angryText, "count": 1}},
			{Class: name, ID: strfmt.UUID(uuid.NewString()), Properties: map[string]any{"text": calmText, "count": 2}},
			{Class: name, ID: strfmt.UUID(uuid.NewString()), Properties: map[string]any{"text": "", "count": 3}},
		})
	}
	createClass(t, ticketsClass, map[string]any{"decisions-dummy": map[string]any{}})
	defer helper.DeleteClass(t, ticketsClass)

	questions := &pb.Decisions{Questions: []*pb.DecisionQuestion{
		predicate("angry", "text", "customer angry"),
		choice("team", "text", "billing", "other"),
		score("length", "text", "short", "medium", "long"),
		predicate("no", "text", "refuse"),
	}}

	t.Run("fetch objects: every object gets one answer per question", func(t *testing.T) {
		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
			Collection: ticketsClass,
			Properties: &pb.PropertiesRequest{NonRefProperties: []string{"text"}},
			Decide:     questions,
		})

		require.Len(t, reply.Results, 3)
		answers := byText(t, reply)

		angry := answers[angryText]
		assert.Equal(t, 1.0, angry["angry"].GetPredicate().GetProbability())
		assert.Equal(t, "billing", angry["team"].GetChoice().GetChoice())
		assert.Equal(t, 1.0, angry["team"].GetChoice().GetConfidence())
		require.Len(t, angry["team"].GetChoice().GetProbabilities(), 2)
		assert.Equal(t, float64(len(angryText)%3), angry["length"].GetScore().GetScore())
		require.Len(t, angry["length"].GetScore().GetProbabilities(), 3)
		assert.Equal(t, "short", angry["length"].GetScore().GetProbabilities()[0].GetValue())
		assert.NotNil(t, angry["no"].GetRefusal(), "the instructions \"refuse\" refuse")

		calm := answers[calmText]
		assert.Equal(t, 0.0, calm["angry"].GetPredicate().GetProbability())
		assert.Equal(t, "billing", calm["team"].GetChoice().GetChoice(), "no option in the text: the first one")

		// An object whose property is empty is not judged: every answer is a refusal.
		empty := answers[""]
		for _, name := range []string{"angry", "team", "length", "no"} {
			assert.NotNil(t, empty[name].GetRefusal(), "question %q", name)
		}
	})

	t.Run("bm25 and a single question", func(t *testing.T) {
		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
			Collection: ticketsClass,
			Bm25Search: &pb.BM25{Query: "billing good"},
			Properties: &pb.PropertiesRequest{NonRefProperties: []string{"text"}},
			Decide:     &pb.Decisions{Questions: questions.Questions[:1]},
		})

		require.Len(t, reply.Results, 2)
		answers := byText(t, reply)
		require.Len(t, answers[angryText], 1)
		assert.Equal(t, 1.0, answers[angryText]["angry"].GetPredicate().GetProbability())
		assert.Equal(t, 0.0, answers[calmText]["angry"].GetPredicate().GetProbability())
	})

	t.Run("without decide the reply carries no decisions", func(t *testing.T) {
		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{Collection: ticketsClass})

		for _, result := range reply.Results {
			assert.Nil(t, result.Decisions)
		}
	})

	t.Run("errors before the search", func(t *testing.T) {
		manyLevels := make([]string, 21)
		for i := range manyLevels {
			manyLevels[i] = strings.Repeat("l", i+1)
		}
		tests := []struct {
			name    string
			req     *pb.SearchRequest
			wantErr string
		}{
			{
				name:    "with group by",
				req:     &pb.SearchRequest{Collection: ticketsClass, GroupBy: &pb.GroupBy{Path: []string{"text"}, NumberOfGroups: 1, ObjectsPerGroup: 1}, Decide: questions},
				wantErr: "decide is not supported with group_by",
			},
			{
				name:    "a question about a property of another type",
				req:     &pb.SearchRequest{Collection: ticketsClass, Decide: &pb.Decisions{Questions: []*pb.DecisionQuestion{predicate("q", "count", "many")}}},
				wantErr: `question "q": property "count" is of type "int", decisions-dummy judges [text]`,
			},
			{
				name:    "a question about a property the class does not have",
				req:     &pb.SearchRequest{Collection: ticketsClass, Decide: &pb.Decisions{Questions: []*pb.DecisionQuestion{predicate("q", "nope", "x")}}},
				wantErr: `question "q": class "DecideTickets" has no property "nope"`,
			},
			{
				name:    "more levels than the ceiling",
				req:     &pb.SearchRequest{Collection: ticketsClass, Decide: &pb.Decisions{Questions: []*pb.DecisionQuestion{score("q", "text", manyLevels...)}}},
				wantErr: `question "q": needs between 2 and 20 levels, got 21`,
			},
			{
				name:    "two questions with one name",
				req:     &pb.SearchRequest{Collection: ticketsClass, Decide: &pb.Decisions{Questions: []*pb.DecisionQuestion{predicate("q", "text", "a"), predicate("q", "text", "b")}}},
				wantErr: `question name "q" is used twice`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := grpchelper.Search(t, tt.req)

				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			})
		}
	})

	t.Run("the page is bounded by the module's maxDocuments", func(t *testing.T) {
		const smallPage = "DecideSmallPage"
		createClass(t, smallPage, map[string]any{"decisions-dummy": map[string]any{"maxDocuments": 2}})
		defer helper.DeleteClass(t, smallPage)

		_, err := grpchelper.Search(t, &pb.SearchRequest{Collection: smallPage, Limit: 3, Decide: questions})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "the page ends at 3, beyond maxDocuments 2 of decisions-dummy")

		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{Collection: smallPage, Limit: 2, Decide: questions})
		assert.Len(t, reply.Results, 2)
	})

	t.Run("a class without a decisions module", func(t *testing.T) {
		const plain = "DecidePlain"
		createClass(t, plain, nil)
		defer helper.DeleteClass(t, plain)

		_, err := grpchelper.Search(t, &pb.SearchRequest{Collection: plain, Decide: questions})

		require.Error(t, err)
		assert.Contains(t, err.Error(), `class "DecidePlain" has no decisions module`)
	})

	t.Run("a reranker and a decisions module on one class", func(t *testing.T) {
		const both = "DecideBoth"
		createClass(t, both, map[string]any{"reranker-dummy": map[string]any{}, "decisions-dummy": map[string]any{}})
		defer helper.DeleteClass(t, both)

		// The reranker serves rerank; both dummies score by length, so the
		// longest text comes first.
		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
			Collection: both,
			Bm25Search: &pb.BM25{Query: "billing good"},
			Properties: &pb.PropertiesRequest{NonRefProperties: []string{"text"}},
			Rerank:     &pb.Rerank{Property: "text"},
			Decide:     &pb.Decisions{Questions: questions.Questions[:1]},
		})

		require.Len(t, reply.Results, 2)
		assert.Equal(t, angryText, reply.Results[0].Properties.NonRefProps.Fields["text"].GetTextValue())
		assert.Equal(t, float64(len(angryText)), reply.Results[0].Metadata.RerankScore)
		assert.Equal(t, float64(len(calmText)), reply.Results[1].Metadata.RerankScore)
		answers := byText(t, reply)
		assert.Equal(t, 1.0, answers[angryText]["angry"].GetPredicate().GetProbability())
	})

	t.Run("two modules of one kind are rejected at create", func(t *testing.T) {
		for _, modules := range []map[string]any{
			{"reranker-dummy": map[string]any{}, "reranker-cohere": map[string]any{}},
			{"decisions-dummy": map[string]any{}, "decisions-typesafeai": map[string]any{}},
		} {
			class := &models.Class{Class: "DecideTwoOfAKind", Vectorizer: "none", ModuleConfig: modules}
			params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)

			_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)

			var unprocessable *schema.SchemaObjectsCreateUnprocessableEntity
			require.ErrorAs(t, err, &unprocessable)
			assert.Contains(t, unprocessable.Payload.Error[0].Message, "at most one is allowed")
		}
	})
}
