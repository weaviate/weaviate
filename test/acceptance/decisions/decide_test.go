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
	// A 1x1 PNG, as a blob property holds it.
	photoBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNgYGAAAAAEAAH2FzhVAAAAAElFTkSuQmCC"
	calmText    = "all good thanks"
)

func ptr[T any](v T) *T { return &v }

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
				{Name: "photo", DataType: []string{"blob"}},
			},
			ModuleConfig: moduleConfig,
		})
		helper.CreateObjectsBatch(t, []*models.Object{
			{Class: name, ID: strfmt.UUID(uuid.NewString()), Properties: map[string]any{"text": angryText, "count": 1, "photo": photoBase64}},
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
				wantErr: `question "q": property "count" is of type "int", decisions-dummy judges [text blob]`,
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
		assert.Contains(t, err.Error(), "limit 3 exceeds maxDocuments 2 of decisions-dummy")

		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{Collection: smallPage, Limit: 2, Decide: questions})
		assert.Len(t, reply.Results, 2)

		// The module judges the page, so the offset does not count.
		reply = grpchelper.AssertSearch(t, &pb.SearchRequest{Collection: smallPage, Limit: 2, Offset: 1, Decide: questions})
		assert.Len(t, reply.Results, 2)
	})

	// decisions-dummy judges a blob as the text of its base64, so the answer
	// is a probability of 0 for the object with a photo and a refusal for
	// the others, whose photo is empty.
	t.Run("a question about an image property goes to the module", func(t *testing.T) {
		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
			Collection: ticketsClass,
			Properties: &pb.PropertiesRequest{NonRefProperties: []string{"text"}},
			Decide:     &pb.Decisions{Questions: []*pb.DecisionQuestion{predicate("damaged", "photo", "damaged")}},
		})

		answers := byText(t, reply)
		require.Len(t, answers, 3)
		assert.Equal(t, 0.0, answers[angryText]["damaged"].GetPredicate().GetProbability())
		assert.NotNil(t, answers[calmText]["damaged"].GetRefusal())
	})

	t.Run("a class without a decisions module", func(t *testing.T) {
		const plain = "DecidePlain"
		createClass(t, plain, nil)
		defer helper.DeleteClass(t, plain)

		_, err := grpchelper.Search(t, &pb.SearchRequest{Collection: plain, Decide: questions})

		require.Error(t, err)
		assert.Contains(t, err.Error(), `class "DecidePlain" has no decisions module`)
	})

	t.Run("a rerank by a decision about an image property goes to the module", func(t *testing.T) {
		// decisions-dummy judges the blob as the text of its base64, which
		// shares no word with the query, so every score is 0 and nothing is
		// dropped; the point is that the blob reaches the module's rerank.
		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
			Collection: ticketsClass,
			Properties: &pb.PropertiesRequest{NonRefProperties: []string{"text"}},
			Rerank:     &pb.Rerank{Property: "photo", Query: ptr("damaged"), Decision: &pb.RerankDecision{}},
		})

		require.Len(t, reply.Results, 3)
		for _, result := range reply.Results {
			assert.Equal(t, 0.0, result.Metadata.RerankScore)
		}
	})

	t.Run("rerank by a decision", func(t *testing.T) {
		texts := func(reply *pb.SearchReply) []string {
			out := make([]string, len(reply.Results))
			for i, result := range reply.Results {
				out[i] = result.Properties.NonRefProps.Fields["text"].GetTextValue()
			}
			return out
		}
		properties := &pb.PropertiesRequest{NonRefProperties: []string{"text"}}

		t.Run("the class settings are the defaults: search order, nothing dropped", func(t *testing.T) {
			reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
				Collection: ticketsClass, Properties: properties,
				Rerank: &pb.Rerank{Property: "text", Query: ptr("customer angry"), Decision: &pb.RerankDecision{}},
			})

			require.Len(t, reply.Results, 3)
			byText := map[string]float64{}
			for _, result := range reply.Results {
				byText[result.Properties.NonRefProps.Fields["text"].GetTextValue()] = result.Metadata.RerankScore
			}
			assert.Equal(t, map[string]float64{angryText: 1, calmText: 0, "": 0}, byText)
		})

		t.Run("a threshold drops the candidates below it", func(t *testing.T) {
			reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
				Collection: ticketsClass, Properties: properties,
				Rerank: &pb.Rerank{Property: "text", Query: ptr("customer angry"), Decision: &pb.RerankDecision{MinProbability: ptr(0.6)}},
			})

			assert.Equal(t, []string{angryText}, texts(reply))
			assert.Equal(t, 1.0, reply.Results[0].Metadata.RerankScore)
		})

		t.Run("order by probability", func(t *testing.T) {
			reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
				Collection: ticketsClass, Properties: properties,
				Rerank: &pb.Rerank{Property: "text", Query: ptr("good thanks customer"), Decision: &pb.RerankDecision{
					MinProbability: ptr(0.3), Order: pb.RerankOrder_RERANK_ORDER_PROBABILITY.Enum(),
				}},
			})

			// "all good thanks" matches two of three words, the angry text one.
			assert.Equal(t, []string{calmText, angryText}, texts(reply))
		})

		t.Run("a plain rerank on a class with only a decisions module is a relevance predicate", func(t *testing.T) {
			reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
				Collection: ticketsClass, Properties: properties,
				Rerank: &pb.Rerank{Property: "text", Query: ptr("customer angry")},
			})

			require.Len(t, reply.Results, 3)
			// The search order is the object order, so the score is read by text.
			byText := map[string]float64{}
			for _, result := range reply.Results {
				byText[result.Properties.NonRefProps.Fields["text"].GetTextValue()] = result.Metadata.RerankScore
			}
			assert.Equal(t, map[string]float64{angryText: 1, calmText: 0, "": 0}, byText)
		})

		t.Run("the class setting applies and the request overrides it", func(t *testing.T) {
			const strict = "DecideStrict"
			createClass(t, strict, map[string]any{"decisions-dummy": map[string]any{"minProbability": 0.9, "order": "probability"}})
			defer helper.DeleteClass(t, strict)

			reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
				Collection: strict, Properties: properties,
				Rerank: &pb.Rerank{Property: "text", Query: ptr("good thanks customer"), Decision: &pb.RerankDecision{}},
			})
			assert.Empty(t, reply.Results, "no text has every word")

			reply = grpchelper.AssertSearch(t, &pb.SearchRequest{
				Collection: strict, Properties: properties,
				Rerank: &pb.Rerank{Property: "text", Query: ptr("good thanks customer"), Decision: &pb.RerankDecision{MinProbability: ptr(0.3)}},
			})
			assert.Equal(t, []string{calmText, angryText}, texts(reply), "the class order probability still applies")
		})

		t.Run("a decision on a class without a decisions module", func(t *testing.T) {
			const rerankerOnly = "DecideRerankerOnly"
			createClass(t, rerankerOnly, map[string]any{"reranker-dummy": map[string]any{}})
			defer helper.DeleteClass(t, rerankerOnly)

			_, err := grpchelper.Search(t, &pb.SearchRequest{
				Collection: rerankerOnly,
				Rerank:     &pb.Rerank{Property: "text", Query: ptr("x"), Decision: &pb.RerankDecision{}},
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), `rerank.decision: class "DecideRerankerOnly" has no decisions module`)
		})

		t.Run("a threshold out of range fails before the search", func(t *testing.T) {
			_, err := grpchelper.Search(t, &pb.SearchRequest{
				Collection: ticketsClass,
				Rerank:     &pb.Rerank{Property: "text", Query: ptr("x"), Decision: &pb.RerankDecision{MinProbability: ptr(1.5)}},
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "rerank.decision.min_probability must be between 0 and 1, got 1.5")
		})

		t.Run("a limit above the module's maxDocuments fails before the search", func(t *testing.T) {
			const smallPage = "DecideSmallPageRerank"
			createClass(t, smallPage, map[string]any{"decisions-dummy": map[string]any{"maxDocuments": 2}})
			defer helper.DeleteClass(t, smallPage)

			_, err := grpchelper.Search(t, &pb.SearchRequest{
				Collection: smallPage, Limit: 3,
				Rerank: &pb.Rerank{Property: "text", Query: ptr("x"), Decision: &pb.RerankDecision{}},
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "rerank.decision: limit 3 exceeds maxDocuments 2 of decisions-dummy")
		})
	})

	t.Run("a reranker and a decisions module on one class", func(t *testing.T) {
		const both = "DecideBoth"
		createClass(t, both, map[string]any{"reranker-dummy": map[string]any{}, "decisions-dummy": map[string]any{}})
		defer helper.DeleteClass(t, both)
		properties := &pb.PropertiesRequest{NonRefProperties: []string{"text"}}

		// A plain rerank goes to the reranker, which scores by length, so
		// the longest text comes first.
		reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
			Collection: both,
			Bm25Search: &pb.BM25{Query: "billing good"},
			Properties: properties,
			Rerank:     &pb.Rerank{Property: "text", Query: ptr("customer angry")},
			Decide:     &pb.Decisions{Questions: questions.Questions[:1]},
		})

		require.Len(t, reply.Results, 2)
		assert.Equal(t, angryText, reply.Results[0].Properties.NonRefProps.Fields["text"].GetTextValue())
		assert.Equal(t, float64(len(angryText)), reply.Results[0].Metadata.RerankScore)
		assert.Equal(t, float64(len(calmText)), reply.Results[1].Metadata.RerankScore)
		answers := byText(t, reply)
		assert.Equal(t, 1.0, answers[angryText]["angry"].GetPredicate().GetProbability())

		// A decision goes to the decisions module: the probability is the
		// score, and the threshold drops the calm text.
		reply = grpchelper.AssertSearch(t, &pb.SearchRequest{
			Collection: both,
			Bm25Search: &pb.BM25{Query: "billing good"},
			Properties: properties,
			Rerank:     &pb.Rerank{Property: "text", Query: ptr("customer angry"), Decision: &pb.RerankDecision{MinProbability: ptr(0.6)}},
		})

		require.Len(t, reply.Results, 1)
		assert.Equal(t, angryText, reply.Results[0].Properties.NonRefProps.Fields["text"].GetTextValue())
		assert.Equal(t, 1.0, reply.Results[0].Metadata.RerankScore)
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
