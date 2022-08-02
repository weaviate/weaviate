package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate-go-client/v2/weaviate"
	"github.com/semi-technologies/weaviate/entities/models"
)

var client *weaviate.Client

func init() {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client = weaviate.New(cfg)
}

type runBlueprint struct {
	name   string
	filter string
	count  int
}

var runBlueprints = [][]runBlueprint{
	singleLevelFilters("Equal", []int{1, 10, 25, 50, 75, 99, 100}, 100),
	chainedFilters("And", [][]int{[]int{100, 1}, []int{1, 100}}, 100),
	chainedFilters("Or", [][]int{[]int{100, 1}, []int{1, 100}}, 100),
	singleLevelFiltersWildcard("Like", []int{1, 10, 25, 50, 75, 99, 100}, 100),
}

func chainedFilters(operator string, matching [][]int, count int) []runBlueprint {
	out := make([]runBlueprint, len(matching))
	for i := range out {

		operands := make([]string, len(matching[i]))
		name := "Chained: "
		for j := range matching[i] {
			operands[j] = singleLevelFilter("Equal", fmt.Sprintf("%d-percent-occurring", matching[i][j]))
			if j != 0 {
				name += fmt.Sprintf(" %s ", operator)
			}
			name += fmt.Sprintf("str=%d%%", matching[i][j])
		}

		out[i] = runBlueprint{
			filter: chainedFilter(operator, operands...),
			count:  count,
			name:   name,
		}
	}

	return out
}

func singleLevelFilters(op string, matching []int, count int) []runBlueprint {
	out := make([]runBlueprint, len(matching))
	for i := range out {
		out[i] = runBlueprint{
			filter: singleLevelFilter(op, fmt.Sprintf("%d-percent-occurring", matching[i])),
			count:  count,
			name:   fmt.Sprintf("Single-Level, %s, %d%% matched", op, matching[i]),
		}
	}

	return out
}

func singleLevelFiltersWildcard(op string, matching []int, count int) []runBlueprint {
	out := make([]runBlueprint, len(matching))
	for i := range out {
		out[i] = runBlueprint{
			filter: singleLevelFilter(op, fmt.Sprintf("%d-percent-occurring-*", matching[i])),
			count:  count,
			name:   fmt.Sprintf("Single-Level with Wildcard, %s, %d%% matched", op, matching[i]),
		}
	}

	return out
}

func singleLevelFilter(op string, value string) string {
	return fmt.Sprintf(`{
					operator:%s
					path: ["string_matches"]
					valueString: "%s"
		} `, op, value)
}

func chainedFilter(operator string, operands ...string) string {
	filter := fmt.Sprintf(`{
					operator:%s
					operands:[`, operator)

	for i, op := range operands {
		if i != 0 {
			filter += ",\n"
		}

		filter += op
	}

	filter += `]}`

	return filter
}

func makeRuns() ([]BenchmarkResult, error) {
	var mergedRunBlueprints []runBlueprint

	for _, section := range runBlueprints {
		mergedRunBlueprints = append(mergedRunBlueprints, section...)
	}

	out := make([]BenchmarkResult, len(mergedRunBlueprints))

	for runId, run := range mergedRunBlueprints {
		vecs := make([][]float32, run.count)
		for i := range vecs {
			vecs[i] = make([]float32, 384)
			for j := range vecs[i] {
				vecs[i][j] = rand.Float32()
			}
		}

		res, err := retrieveAndMeassure(run.name, vecs, run.filter)
		if err != nil {
			return nil, err
		}

		out[runId] = res
	}
	return out, nil
}

func retrieveAndMeassure(name string, vecs [][]float32, filter string) (BenchmarkResult, error) {
	m := &Measurements{}

	for _, vec := range vecs {
		before := time.Now()
		_, err := retrieveFromWeaviate(vec, 10, filter)
		if err != nil {
			return BenchmarkResult{}, err
		}
		m.Add(time.Since(before))
	}

	return m.BenchmarkResult(name), nil
}

func retrieveFromWeaviate(vec []float32, limit int, filter string) ([]obj, error) {
	ctx := context.Background()

	result, err := client.GraphQL().Get().Objects().
		WithClassName("DemoClass").
		WithFields("_additional { id }").
		WithNearVector(fmt.Sprintf("{vector: %v}", vec)).
		WithWhere(filter).
		WithLimit(limit).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return parseGraphQLRes(result, "DemoClass")
}

func parseGraphQLRes(in *models.GraphQLResponse, className string) ([]obj, error) {
	if len(in.Errors) > 0 {
		return nil, errors.Errorf("%s", in.Errors[0].Message)
	}

	list := in.Data["Get"].(map[string]interface{})[className].([]interface{})

	out := make([]obj, len(list))

	for i, elem := range list {
		id := elem.(map[string]interface{})["_additional"].(map[string]interface{})["id"]
		out[i] = obj{ID: id.(string)}
	}

	return out, nil
}

type obj struct {
	Properties map[string]interface{} `json:"properties"`
	Vector     []float32              `json:"vector"`
	ID         string                 `json:"id"`
}
