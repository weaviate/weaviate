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

package batched_contains_tests

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
)

const (
	className = "BatchedContains"

	// objectCount is only the corpus size. What decides whether the fold runs
	// on one worker or several is the number of values a filter names, and
	// those need not exist — see splitValues.
	objectCount = 300

	// splitValues names more values than one worker's minimum share, so the
	// planner splits the batch. Absent keys are deliberate: only the first
	// objectCount of them match anything, so the tail exercises rows the fold
	// reads and finds empty.
	splitValues = 128

	// sparseValues names far more values than exist, so the fold reads rows
	// that are not there. More workers mean a smaller share each, so this is
	// also what keeps a share above the accumulator gate: at the pinned four
	// workers 2048/4 clears it, where 1024/4 lands exactly on it.
	sparseValues = 2048

	// commonTags are carried by every object, so a ContainsAll over them has a
	// non-empty answer to find across more keys than one worker takes.
	commonTags = 64
)

// TestBatchedContains runs the Contains operators over the batched fold through
// the real API, and checks each answer against a set computed from the seeded
// data.
//
// What this covers that a unit test cannot is the wire path: the schema the
// batch gate inspects, the tokenization it insists on, and the GraphQL layer
// that normalizes a filter's value types before the searcher ever sees them.
//
// It deliberately does not re-check that the fold answers what the desugared
// path answers. TestDocIDs_BatchedMatchesDesugared already pins that at the DB
// level over a larger corpus and more case shapes, and repeating it here would
// cost a second instance to make a weaker version of the same statement.
func TestBatchedContains(t *testing.T) {
	ctx := context.Background()

	weaviate := startWeaviate(t, ctx)
	seed(t, ctx, weaviate.client)

	base := seedTime()

	// Value lists. Every one names at least two values, below which the batch
	// gate declines before a fold is ever planned.
	tagsUpTo := func(n int) []string { return valuesUpTo(n, tagOf) }
	codesUpTo := func(n int) []string { return valuesUpTo(n, codeOf) }
	commonUpTo := func(n int) []string { return valuesUpTo(n, commonTagOf) }
	intsUpTo := func(n int) []int64 { return valuesUpTo(n, func(i int) int64 { return int64(i) }) }
	scoresUpTo := func(n int) []float64 { return valuesUpTo(n, scoreOf) }
	datesUpTo := func(n int) []time.Time {
		return valuesUpTo(n, func(i int) time.Time { return whenOf(base, i) })
	}

	tests := []struct {
		name string
		// where is the filter under test. It must be a Contains operator on a
		// property the batch gate accepts, or the case proves nothing.
		where *filters.WhereBuilder
		// match reports whether the object seeded at index i belongs in the
		// answer, derived from the seed rather than from a second query
		match func(i int) bool
	}{
		// text, field tokenization — the shape the fold was built for
		{
			name: "ContainsAny over a single text property",
			where: filters.Where().WithPath([]string{"tag"}).
				WithOperator(filters.ContainsAny).
				WithValueText(tagsUpTo(splitValues)...),
			match: func(i int) bool { return i < splitValues },
		},
		{
			name: "ContainsNone over a single text property",
			where: filters.Where().WithPath([]string{"tag"}).
				WithOperator(filters.ContainsNone).
				WithValueText(tagsUpTo(splitValues)...),
			match: func(i int) bool { return i >= splitValues },
		},
		{
			name: "ContainsAny over a text array property",
			where: filters.Where().WithPath([]string{"tags"}).
				WithOperator(filters.ContainsAny).
				WithValueText(tagsUpTo(splitValues)...),
			match: func(i int) bool { return i < splitValues },
		},
		{
			name: "ContainsAny over mostly absent values",
			where: filters.Where().WithPath([]string{"tag"}).
				WithOperator(filters.ContainsAny).
				WithValueText(tagsUpTo(sparseValues)...),
			match: func(i int) bool { return true },
		},
		{
			name: "ContainsNone over mostly absent values",
			where: filters.Where().WithPath([]string{"tag"}).
				WithOperator(filters.ContainsNone).
				WithValueText(tagsUpTo(sparseValues)...),
			match: func(i int) bool { return false },
		},

		// ContainsAll, which folds by intersection and can settle early
		{
			name: "ContainsAll over values every object carries",
			where: filters.Where().WithPath([]string{"tags"}).
				WithOperator(filters.ContainsAll).
				WithValueText(commonUpTo(commonTags)...),
			match: func(i int) bool { return true },
		},
		{
			name: "ContainsAll narrowed to one object by its own tag",
			where: filters.Where().WithPath([]string{"tags"}).
				WithOperator(filters.ContainsAll).
				WithValueText(append(commonUpTo(commonTags), tagOf(0))...),
			match: func(i int) bool { return i == 0 },
		},
		{
			name: "ContainsAll of two tags no object carries together",
			where: filters.Where().WithPath([]string{"tags"}).
				WithOperator(filters.ContainsAll).
				WithValueText(tagOf(0), tagOf(1)),
			match: func(i int) bool { return false },
		},
		{
			name: "ContainsAll settling empty part-way through a split batch",
			where: filters.Where().WithPath([]string{"tags"}).
				WithOperator(filters.ContainsAll).
				WithValueText(tagsUpTo(splitValues)...),
			match: func(i int) bool { return false },
		},

		// uuid, which batches as its own key type
		{
			name: "ContainsAny over a uuid property",
			where: filters.Where().WithPath([]string{"code"}).
				WithOperator(filters.ContainsAny).
				WithValueText(codesUpTo(splitValues)...),
			match: func(i int) bool { return i < splitValues },
		},

		// primitives, which batch without tokenization
		{
			name: "ContainsAny over an int property",
			where: filters.Where().WithPath([]string{"num"}).
				WithOperator(filters.ContainsAny).
				WithValueInt(intsUpTo(splitValues)...),
			match: func(i int) bool { return i < splitValues },
		},
		{
			name: "ContainsNone over an int property",
			where: filters.Where().WithPath([]string{"num"}).
				WithOperator(filters.ContainsNone).
				WithValueInt(intsUpTo(splitValues)...),
			match: func(i int) bool { return i >= splitValues },
		},
		{
			name: "ContainsAll over an int array property",
			where: filters.Where().WithPath([]string{"nums"}).
				WithOperator(filters.ContainsAll).
				WithValueInt(sharedNums...),
			match: func(i int) bool { return true },
		},
		{
			name: "ContainsAny over a number property",
			where: filters.Where().WithPath([]string{"score"}).
				WithOperator(filters.ContainsAny).
				WithValueNumber(scoresUpTo(splitValues)...),
			match: func(i int) bool { return i < splitValues },
		},
		{
			name: "ContainsAny over a date property",
			where: filters.Where().WithPath([]string{"when"}).
				WithOperator(filters.ContainsAny).
				WithValueDate(datesUpTo(splitValues)...),
			match: func(i int) bool { return i < splitValues },
		},
		{
			// two values is the whole domain of a bool, so this one can never
			// split. It is here because the gate admits it and the fold must
			// still answer on one worker.
			name: "ContainsAny over a boolean property",
			where: filters.Where().WithPath([]string{"flag"}).
				WithOperator(filters.ContainsAny).
				WithValueBoolean(true, false),
			match: func(i int) bool { return true },
		},
	}

	// The whole table runs twice. Seeded objects sit in the active memtable
	// until the dirty-flush timer fires, so a single pass reads one tier and
	// leaves the fold's segment readers — the half the windowing exists for —
	// with no end-to-end coverage at all.
	for _, tier := range []string{"in the memtable", "on disk"} {
		if tier == "on disk" {
			awaitFlush(t, ctx, weaviate)
		}

		// Baseline per tier, read after awaitFlush: awaitFlush polls with
		// batched queries of its own, whose folds a baseline taken before the
		// loop would count.
		base, err := weaviate.readLogs(ctx)
		require.NoError(t, err)
		foldsBefore, readsBefore := len(foldWorkers(base)), len(memtableReads(base))

		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s, %s", tt.name, tier), func(t *testing.T) {
				want := expectedIDs(tt.match)

				got := queryIDs(t, ctx, weaviate.client, tt.where)
				require.ElementsMatch(t, want, got,
					"batched fold disagrees with the seeded data")
			})
		}

		// Waited for by count, not by token: every token these queries write is
		// already in the stream from the tier before, or from awaitFlush.
		after := weaviate.logsAfterFolds(t, ctx, foldsBefore+len(tests))

		require.GreaterOrEqual(t, len(foldWorkers(after))-foldsBefore, len(tests),
			"every case in this tier must have planned a fold of its own")

		// awaitFlush proved every row is in a segment, so a fold reading a
		// memtable here ran against the wrong storage. Every one of them, not
		// the last: a trailing zero says nothing about the ones before it.
		// The memtable tier gets no such assertion — whether the dirty timer
		// has fired by then is a race.
		if tier == "on disk" {
			reads := memtableReads(after)[readsBefore:]
			require.NotEmpty(t, reads, "no fold in the disk tier reported memtable_reads")
			for i, r := range reads {
				require.Zerof(t, r, "fold %d of the disk tier read a memtable", i)
			}
		}
	}

	// A correct answer says nothing about which code produced it, and one
	// property left on the text default is enough to send its cases down the
	// desugared path with every assertion above still green. The slow-query log
	// is the only evidence from outside the process of which path ran.
	t.Run("the batched fold answered every case", func(t *testing.T) {
		logs := weaviate.logs(t, ctx, "fold_strategy")

		// Exact, not indicative: desugaredContains writes this key for any
		// Contains filter it declines, the seed runs none, and nothing else in
		// the process writes it. One case slipping is one occurrence.
		require.NotContains(t, logs, "contains_desugared",
			"a Contains filter fell back to the desugared per-value path")

		require.Contains(t, logs, "intersection", "no ContainsAll folded by intersection")
		// Both by name: "union-" is a prefix of each, and the accumulator is
		// the one arm exercising the Accumulator API the sroar bump added.
		require.Contains(t, logs, "union-incremental", "no small union folded incrementally")
		require.Contains(t, logs, "union-accumulator", "no large union reached the Accumulator")

		// The container's core count is pinned, so this is a property of the
		// planner rather than of the machine CI happened to schedule.
		require.Greater(t, slices.Max(foldWorkers(logs)), 1,
			"every fold ran on one worker, so the batch was never split")
	})

	// The accepting side of a gate proves nothing about the refusing side. A
	// gate widened to admit word tokenization would return wrong answers while
	// every case above stayed green.
	t.Run("a property the gate refuses falls back and still answers", func(t *testing.T) {
		before := weaviate.logs(t, ctx, "fold_strategy")

		got := queryIDs(t, ctx, weaviate.client,
			filters.Where().WithPath([]string{"wordTag"}).
				WithOperator(filters.ContainsAny).
				WithValueText(tagsUpTo(splitValues)...))
		require.ElementsMatch(t, expectedIDs(func(i int) bool { return i < splitValues }), got,
			"the desugared path must answer what the batched one does")

		added := strings.TrimPrefix(
			weaviate.logs(t, ctx, "tokenization-not-field"), before)
		require.Contains(t, added, "contains_desugared",
			"a word-tokenized property must not batch")
		require.Contains(t, added, "tokenization-not-field",
			"and must say which check refused it")
	})

	// Two callers reach the fold by a route Get does not: batch delete through
	// DocIDsLimited, and Aggregate through a Searcher of its own. Aggregate
	// cannot be checked against the log: AnnotateSlowQueryLog discards the
	// fold's fields unless the context carries slow_query_details, which its
	// path does not install.
	t.Run("batch delete answers through DocIDsLimited", func(t *testing.T) {
		// Dry run, so the corpus every other case reads is untouched.
		resp, err := weaviate.client.Batch().ObjectsBatchDeleter().
			WithClassName(className).
			WithDryRun(true).
			WithOutput("minimal").
			WithWhere(filters.Where().WithPath([]string{"tag"}).
				WithOperator(filters.ContainsAny).
				WithValueText(tagsUpTo(splitValues)...)).
			Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp.Results)
		require.EqualValues(t, splitValues, resp.Results.Matches,
			"batch delete must match what the same filter returns through Get")

		// The query names itself in the slow-query log, so a delete records
		// which resolver produced its set.
		weaviate.logsMatching(t, ctx, findUUIDsQueryRE)
	})

	t.Run("aggregate answers through its own allow list", func(t *testing.T) {
		resp, err := weaviate.client.GraphQL().Aggregate().
			WithClassName(className).
			WithWhere(filters.Where().WithPath([]string{"tag"}).
				WithOperator(filters.ContainsAny).
				WithValueText(tagsUpTo(splitValues)...)).
			WithFields(graphql.Field{
				Name:   "meta",
				Fields: []graphql.Field{{Name: "count"}},
			}).
			Do(ctx)
		require.NoError(t, err)
		require.Empty(t, resp.Errors)
		require.EqualValues(t, splitValues, aggregateCount(t, resp),
			"aggregate must count what the same filter returns through Get")
	})

	// The searcher can batch a len() Contains filter, and no supported API can
	// send one: ValidateFilters admits only (not) equal and greater/less than
	// (equal) on a length path. This pins that boundary, so widening the
	// whitelist fails here until the shape gets coverage of its own.
	//
	// The class does index property lengths, so the refusal is about the
	// operator rather than about the lengths being unavailable.
	t.Run("a len() Contains filter is refused before it reaches the searcher", func(t *testing.T) {
		tagsPerObject := int64(1 + commonTags)

		_, err := queryIDsErr(ctx, weaviate.client,
			filters.Where().WithPath([]string{"len(tags)"}).
				WithOperator(filters.ContainsAny).
				WithValueInt(tagsPerObject, tagsPerObject+1))
		require.ErrorContains(t, err,
			"Filtering for property length supports operators (not) equal and greater/less than (equal)",
			"a len() Contains filter must be refused by the operator whitelist, not by anything else")

		matched := queryIDs(t, ctx, weaviate.client,
			filters.Where().WithPath([]string{"len(tags)"}).
				WithOperator(filters.Equal).
				WithValueInt(tagsPerObject))
		require.Len(t, matched, objectCount,
			"every object carries the same tag count, so Equal must match all of them")
	})

	// Shard.buildAllowList is reached only by a filtered vector search, and no
	// other case here takes it. The filter decides which IDs come back.
	t.Run("a filtered vector search answers through buildAllowList", func(t *testing.T) {
		where := filters.Where().WithPath([]string{"num"}).
			WithOperator(filters.ContainsAny).
			WithValueInt(0, 1, 2)

		resp, err := weaviate.client.GraphQL().Get().
			WithClassName(className).
			WithNearVector(weaviate.client.GraphQL().NearVectorArgBuilder().
				WithVector(vectorOf(0))).
			WithWhere(where).
			WithLimit(objectCount).
			WithFields(graphql.Field{
				Name:   "_additional",
				Fields: []graphql.Field{{Name: "id"}},
			}).
			Do(ctx)
		require.NoError(t, err)
		require.Empty(t, resp.Errors)

		require.ElementsMatch(t,
			[]string{idOf(0), idOf(1), idOf(2)},
			acceptance_with_go_client.GetIds(t, resp, className),
			"the vector search must return exactly what the filter matched")
	})
}

// TestBatchedContainsDisabled runs the one setting that turns the fold off.
// Every other case here proves the default batches, which says nothing about
// whether a deployment can still opt out — the lever an operator reaches for
// when the fold is the suspect.
func TestBatchedContainsDisabled(t *testing.T) {
	ctx := context.Background()

	weaviate := startWeaviate(t, ctx, "QUERY_BATCHED_CONTAINS_ENABLED", "false")
	seed(t, ctx, weaviate.client)

	got := queryIDs(t, ctx, weaviate.client,
		filters.Where().WithPath([]string{"tag"}).
			WithOperator(filters.ContainsAny).
			WithValueText(valuesUpTo(splitValues, tagOf)...))
	require.Len(t, got, splitValues,
		"the desugared path must answer the same filter the batched one does")

	logs := weaviate.logs(t, ctx, "contains_desugared")
	// the reason containsDeclineNotEnabled renders as; the constant is
	// unexported and this module cannot import it
	require.Contains(t, logs, "not-enabled",
		"the filter desugared, but for some reason other than the setting")
	require.Empty(t, foldWorkers(logs), "a fold ran with the setting turned off")
}

// aggregateCount reads meta.count out of an Aggregate response, asserting its
// way through the shape rather than treating an unfamiliar one as zero.
func aggregateCount(t *testing.T, resp *models.GraphQLResponse) float64 {
	t.Helper()

	agg, ok := resp.Data["Aggregate"].(map[string]interface{})
	require.Truef(t, ok, "response has no Aggregate object: %v", resp.Data)

	list, ok := agg[className].([]interface{})
	require.Truef(t, ok, "response has no %s list: %v", className, agg)
	require.Len(t, list, 1)

	entry, ok := list[0].(map[string]interface{})
	require.Truef(t, ok, "aggregate entry is not an object: %v", list[0])

	meta, ok := entry["meta"].(map[string]interface{})
	require.Truef(t, ok, "aggregate entry has no meta: %v", entry)

	count, ok := meta["count"].(float64)
	require.Truef(t, ok, "meta has no count: %v", meta)
	return count
}

// instance is one running Weaviate: the client to query it with, and the
// container to read back what it did.
type instance struct {
	client    *wvt.Client
	container *docker.DockerContainer
}

// startWeaviate brings up one instance. env carries extra settings as
// name/value pairs; a caller that passes none gets QUERY_BATCHED_CONTAINS_ENABLED
// unset, so the run proves the shipped default still batches.
//
// Slow-query logging is on with a threshold that every query clears, because
// that log is where the fold records the plan it chose. The threshold cannot be
// zero: the reporter reads a non-positive one as unset and substitutes its own
// default, which would log almost nothing.
func startWeaviate(t *testing.T, ctx context.Context, env ...string) *instance {
	t.Helper()
	require.Truef(t, len(env)%2 == 0, "env must be name/value pairs, got %d values", len(env))

	compose := docker.New().
		WithWeaviate()
	for i := 0; i < len(env); i += 2 {
		compose = compose.WithWeaviateEnv(env[i], env[i+1])
	}

	composed, err := compose.
		WithWeaviateEnv("QUERY_SLOW_LOG_ENABLED", "true").
		WithWeaviateEnv("QUERY_SLOW_LOG_THRESHOLD", "1ns").
		// The planner takes min(budget, GOMAXPROCS, keys/32), so without this
		// the worker count is the CI runner's core count and a one-core host
		// reports the batch was never split as if it were a product fault.
		WithWeaviateEnv("GOMAXPROCS", "4").
		// so the seeded objects reach a segment within the test rather than
		// after the default minute
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, composed.Terminate(ctx)) })

	client, err := wvt.NewClient(wvt.Config{
		Scheme: "http", Host: composed.GetWeaviate().URI(),
	})
	require.NoError(t, err)
	return &instance{client: client, container: composed.GetWeaviate()}
}

// readLogs returns everything the instance has written so far. It reports an
// error rather than asserting: it runs inside an Eventually condition, where
// FailNow is a runtime.Goexit, so the poll would report a timeout instead of
// the transport error that caused it.
func (in *instance) readLogs(ctx context.Context) (string, error) {
	reader, err := in.container.Container().Logs(ctx)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	out, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// logsAfterFolds returns the instance's output once it holds at least want
// fold_workers lines, so a caller waits for its own queries.
func (in *instance) logsAfterFolds(t *testing.T, ctx context.Context, want int) string {
	t.Helper()

	var out string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		s, err := in.readLogs(ctx)
		if !assert.NoError(c, err) {
			return
		}
		out = s
		assert.GreaterOrEqual(c, len(foldWorkers(s)), want)
	}, 30*time.Second, 100*time.Millisecond,
		"the container never logged %d folds", want)
	return out
}

// logs returns the instance's output once waitFor appears in it. Docker's
// stdout capture is asynchronous, so a line written before the query returned
// may not have reached the stream yet. waitFor must be a token the caller's own
// query produces, or the first read returns and waits for nothing.
func (in *instance) logs(t *testing.T, ctx context.Context, waitFor string) string {
	t.Helper()

	var out string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		s, err := in.readLogs(ctx)
		if !assert.NoError(c, err) {
			return
		}
		out = s
		assert.Contains(c, s, waitFor)
	}, 10*time.Second, 100*time.Millisecond,
		"the container never logged %q", waitFor)
	return out
}

// foldWorkers returns every fold_workers the logs recorded, oldest first.
//
// Matched out of the raw text rather than by parsing it, so the assertion holds
// under either log format: the field renders as `"fold_workers":4` in JSON and
// `fold_workers:4` inside a formatted map otherwise.
var foldWorkersRE = regexp.MustCompile(`fold_workers"?\s*[:=]\s*"?(\d+)`)

// logsMatching returns the instance's output once re matches it, for a caller
// whose evidence is a log field rather than a bare token.
func (in *instance) logsMatching(t *testing.T, ctx context.Context, re *regexp.Regexp) string {
	t.Helper()

	var out string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		s, err := in.readLogs(ctx)
		if !assert.NoError(c, err) {
			return
		}
		out = s
		assert.Regexp(c, re, s)
	}, 10*time.Second, 100*time.Millisecond,
		"the delete path recorded no slow-query entry, so which resolver ran is unrecoverable")
	return out
}

// findUUIDsQueryRE matches the slow-query entry the delete path writes, under
// either log format, and not the plain debug lines naming the same method.
var findUUIDsQueryRE = regexp.MustCompile(`query"?\s*[:=]\s*"?FindUUIDs`)

func foldWorkers(logs string) []int {
	matches := foldWorkersRE.FindAllStringSubmatch(logs, -1)
	out := make([]int, 0, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		out = append(out, n)
	}
	return out
}

// sharedNums sit in every object's nums array, so a ContainsAll over them
// matches everything while still naming the two values the gate requires.
var sharedNums = []int64{5000, 5001}

func tagOf(i int) string       { return fmt.Sprintf("tag-%04d", i) }
func commonTagOf(i int) string { return fmt.Sprintf("common-%04d", i) }
func idOf(i int) string        { return fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1) }
func codeOf(i int) string      { return fmt.Sprintf("00000000-0000-0000-0001-%012d", i+1) }
func scoreOf(i int) float64    { return float64(i) / 4 } // quarters are exact in binary
func seedTime() time.Time      { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// vectorOf gives each object a distinct vector, so a nearVector search has
// something to score.
func vectorOf(i int) []float32 { return []float32{float32(i), 1, 0, 0} }

func whenOf(b time.Time, i int) time.Time {
	return b.Add(time.Duration(i) * time.Hour)
}

// awaitFlush waits for the seeded objects to reach a segment, which the dirty
// timer does on its own once the memtable has been idle. It is observed rather
// than slept for: the reader reports how many memtables it took the row from,
// and that reaches zero only once nothing is left unflushed.
//
// Nothing in the polled path asserts, for the reason readLogs gives.
func awaitFlush(t *testing.T, ctx context.Context, in *instance) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		// any batched query will do; this one exists to move the log on
		if _, err := queryIDsErr(ctx, in.client,
			filters.Where().WithPath([]string{"tag"}).
				WithOperator(filters.ContainsAny).
				WithValueText(tagOf(0), tagOf(1))); !assert.NoError(c, err) {
			return
		}
		logs, err := in.readLogs(ctx)
		if !assert.NoError(c, err) {
			return
		}
		reads, ok := lastMemtableReads(logs)
		if !assert.True(c, ok, "no fold has reported memtable_reads yet") {
			return
		}
		assert.Zero(c, reads)
	}, 30*time.Second, time.Second,
		"the seeded objects never reached a segment, so the disk pass would "+
			"repeat the memtable one")
}

// memtableReadsRE captures how many memtables a fold read, zero exactly when
// everything the filter touched is in a segment. Matched out of the raw text
// for the reason foldWorkersRE gives.
var memtableReadsRE = regexp.MustCompile(`memtable_reads"?\s*[:=]\s*"?(\d+)`)

// memtableReads returns every count the logs recorded, oldest first.
func memtableReads(logs string) []int {
	matches := memtableReadsRE.FindAllStringSubmatch(logs, -1)
	out := make([]int, 0, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		out = append(out, n)
	}
	return out
}

// lastMemtableReads returns the most recent count — the fold the caller just
// triggered. Only awaitFlush wants this; a tier assertion reads every count.
func lastMemtableReads(logs string) (int, bool) {
	all := memtableReads(logs)
	if len(all) == 0 {
		return 0, false
	}
	return all[len(all)-1], true
}

// valuesUpTo is the first n values a seeded property takes.
func valuesUpTo[T any](n int, of func(int) T) []T {
	out := make([]T, 0, n)
	for i := range n {
		out = append(out, of(i))
	}
	return out
}

// expectedIDs is the answer derived from the seed, in the same form the API
// returns: the IDs of every object the predicate accepts.
func expectedIDs(match func(i int) bool) []string {
	out := make([]string, 0, objectCount)
	for i := range objectCount {
		if match(i) {
			out = append(out, idOf(i))
		}
	}
	return out
}

// seed creates the class and writes the corpus.
//
// Every filterable property is explicitly configured rather than left to the
// defaults: field tokenization on the text properties, because the batch gate
// declines any other, and a filterable index, because it declines a property
// without one. A property that slipped back to the text default would send its
// cases down the desugared path, and they would pass having tested nothing.
func seed(t *testing.T, ctx context.Context, client *wvt.Client) {
	t.Helper()

	vTrue, vFalse := true, false
	text := func(name string, dt schema.DataType) *models.Property {
		return &models.Property{
			Name:            name,
			DataType:        dt.PropString(),
			Tokenization:    models.PropertyTokenizationField,
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
		}
	}
	plain := func(name string, dt schema.DataType) *models.Property {
		return &models.Property{
			Name:            name,
			DataType:        dt.PropString(),
			IndexFilterable: &vTrue,
		}
	}

	class := &models.Class{
		Class: className,
		// vectors are supplied per object, for the one filtered vector search
		Vectorizer: "none",
		// so len(tags) has a length bucket to read
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexPropertyLength: true},
		Properties: []*models.Property{
			text("tag", schema.DataTypeText),
			// the refused shape: word tokenization can turn one value into zero
			// or several tokens, so it is not the 1-value-1-key relation the
			// batch gate needs. Filterable, so the refusal is about the
			// tokenization and nothing else.
			{
				Name:            "wordTag",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
			},
			text("tags", schema.DataTypeTextArray),
			plain("code", schema.DataTypeUUID),
			plain("num", schema.DataTypeInt),
			plain("nums", schema.DataTypeIntArray),
			plain("score", schema.DataTypeNumber),
			plain("when", schema.DataTypeDate),
			plain("flag", schema.DataTypeBoolean),
		},
	}
	require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

	base := seedTime()
	common := make([]string, 0, commonTags)
	for i := range commonTags {
		common = append(common, commonTagOf(i))
	}

	batcher := client.Batch().ObjectsBatcher()
	for i := range objectCount {
		tags := append([]string{tagOf(i)}, common...)

		batcher.WithObjects(&models.Object{
			Class:  className,
			ID:     strfmt.UUID(idOf(i)),
			Vector: vectorOf(i),
			Properties: map[string]interface{}{
				"tag":     tagOf(i),
				"wordTag": tagOf(i),
				"tags":    tags,
				"code":    codeOf(i),
				"num":     i,
				"nums":    append([]int64{int64(i)}, sharedNums...),
				"score":   scoreOf(i),
				"when":    whenOf(base, i).Format(time.RFC3339),
				"flag":    i%2 == 0,
			},
		})
	}

	resp, err := batcher.Do(ctx)
	require.NoError(t, err)
	require.Len(t, resp, objectCount)
	for _, r := range resp {
		require.NotNil(t, r.Result)
		require.Nil(t, r.Result.Errors, "seeding must not partially fail")
	}
}

// queryIDs runs one filter and returns the IDs it matched.
//
// The limit is above the corpus, so a truncated page can never be mistaken for
// a fold that dropped rows — the failure this whole test exists to catch.
func queryIDs(t *testing.T, ctx context.Context, client *wvt.Client,
	where *filters.WhereBuilder,
) []string {
	t.Helper()

	resp, err := queryIDsErr(ctx, client, where)
	require.NoError(t, err)

	if empty(t, resp) {
		return nil
	}
	return acceptance_with_go_client.GetIds(t, resp, className)
}

// queryIDsErr runs the query and reports transport and GraphQL errors rather
// than asserting them, so a caller polling inside an Eventually can retry.
func queryIDsErr(ctx context.Context, client *wvt.Client,
	where *filters.WhereBuilder,
) (*models.GraphQLResponse, error) {
	resp, err := client.GraphQL().Get().
		WithClassName(className).
		WithWhere(where).
		WithFields(graphql.Field{
			Name:   "_additional",
			Fields: []graphql.Field{{Name: "id"}},
		}).
		WithLimit(objectCount + 1).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", resp.Errors)
	}
	return resp, nil
}

// empty reports whether the response carried no objects. GetIds asserts its way
// through the response shape, which a legitimately empty answer does not have —
// so the shape is checked here rather than treating anything unfamiliar as an
// empty answer, which is indistinguishable from a correct one for the cases
// that expect nothing.
func empty(t *testing.T, resp *models.GraphQLResponse) bool {
	t.Helper()

	get, ok := resp.Data["Get"].(map[string]interface{})
	require.Truef(t, ok, "response has no Get object: %v", resp.Data)

	objects, ok := get[className].([]interface{})
	require.Truef(t, ok, "response has no %s list: %v", className, get)
	return len(objects) == 0
}
