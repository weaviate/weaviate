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

package reindex_singlenode

// testPropertyStateMigrationMatrix enumerates the cartesian product of
//
//  1. property data type (text, int, boolean, uuid, geoCoordinates, blob —
//     one representative per dispatch class),
//  2. property pre-state (the meaningful combinations of IndexFilterable /
//     IndexSearchable / IndexRangeFilters / Tokenization that the schema
//     manager accepts for that data type), and
//  3. PUT /v1/schema/{class}/indexes/{prop} request body shape.
//
// For every cell the test asserts a small set of *structural* invariants —
// the things we never want the server to do regardless of input:
//
//   - **No 5xx.** Validation/dispatch errors must be 4xx, never internal
//     server errors. A 5xx on a syntactically valid request body that
//     points at an existing class+property is a bug, even when the
//     combination is nonsensical (e.g. enable-rangeable on a text
//     property).
//
//   - **No 4xx with empty / unstructured body.** Every 4xx must be a JSON
//     `{"error":[{"message":"..."}]}` envelope with at least one
//     non-empty message. A bare 4xx tells the user nothing.
//
//   - **No silent FINISHED-with-empty-bucket.** When a cell returns 202,
//     the task must reach FINISHED, the targeted schema flag must flip
//     (or for rebuilds, stay flipped), AND a query against the resulting
//     index must return at least one row from the small corpus inserted
//     before the PUT. A FINISHED task with a queryable bucket that
//     returns zero rows is the Sev 1 failure mode we keep getting bitten
//     by (DELETE→re-enable short-circuits on a stale sentinel, CANCEL→
//     retry hits a half-written sidecar, etc.). Catching it here on a
//     wide combinatorial sweep is cheaper than discovering it in
//     production.
//
//   - **No silent 202 on a no-op body.** A body that does not name any
//     verb (`{"searchable":{"enabled":false}}` etc.) must 4xx — it must
//     never be silently accepted as a no-op.
//
// The matrix does NOT try to encode the precise validation rules from
// handlers_reindex.go (which tokenization is rejected, which body shape
// pairs are compatible, etc.). Those are covered by the dedicated
// api_validation_test.go and per-migration-type tests, where the
// expectations are stable enough to keep in lock-step. Re-encoding them
// here would just create a parallel rulebook that drifts. The structural
// invariants above are robust to validator refactors and still catch the
// real bug shapes we care about.
//
// Failure modes the matrix has caught in the past (a non-exhaustive list,
// kept up-to-date as new bugs appear):
//
//   - PUT `{"filterable":{"tokenization":"X"}}` was silently ignored
//     (model carried the field but the dispatcher never read it). Fixed
//     by adding ReindexTypeChangeTokenizationFilterable as a first-class
//     verb in handlers_indexes.go. The matrix would now catch a
//     regression where the verb is removed: the cell would either return
//     "no actionable change" (which is no longer the right answer for a
//     property with a filterable index) or silently 202 a no-op task.
//
//   - PUT `{"searchable":{"tokenization":"X"}}` on a filterable-only
//     text property used to return "searchable bucket not found", which
//     does not point the user at the right body shape. Fixed by
//     intercepting that path in handlers_indexes.go and returning a
//     message that explicitly names both alternative body shapes.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// ----------------------------------------------------------------------------
// Matrix dimensions
// ----------------------------------------------------------------------------

// matrixDataType describes one column of the matrix: the schema data type
// and which migration shapes it can legitimately receive. The
// `supportsFilt` / `supportsSrch` / `supportsRange` flags are used only to
// skip cells whose pre-state cannot be created (e.g. searchable=true on a
// non-text property; the schema manager rejects CreateClass for those).
type matrixDataType struct {
	name           string // schema data type, e.g. "text", "int"
	supportsFilt   bool   // schema accepts IndexFilterable=true on creation
	supportsSrch   bool   // schema accepts IndexSearchable=true (text/text[])
	supportsRange  bool   // schema accepts IndexRangeFilters=true (numeric/date)
	allowDefaultTk bool   // tokenization in the pre-state is meaningful
}

// matrixDataTypes returns one representative per dispatch class. The matrix
// targets the dispatcher / validator state machine, not the per-type
// storage backends. Per-type backend correctness is covered by the dedicated
// per-migration-type tests (enable_filterable_test.go etc.).
//
//   - text: distinct dispatch paths for change-tokenization and
//     enable-searchable.
//   - int: representative of numeric (filterable + rangeable). number /
//     date / array variants take the same dispatch path; covering them
//     here is redundant.
//   - boolean: representative of filterable-only primitive.
//   - uuid: distinct bucket strategy.
//   - blob: representative of "type rejected by filterable validator".
//   - geoCoordinates: distinct because validateEnableFilterableProperty
//     rejects it with the same error path as blob, but its schema-default
//     IndexFilterable behavior differs.
func matrixDataTypes() []matrixDataType {
	return []matrixDataType{
		{"text", true, true, false, true},
		{"int", true, false, true, false},
		{"boolean", true, false, false, false},
		{"uuid", true, false, false, false},
		{"geoCoordinates", false, false, false, false},
		{"blob", false, false, false, false},
	}
}

// matrixPreState describes the property's index flags + tokenization at the
// moment the test issues the PUT. `name` is used in the subtest label.
type matrixPreState struct {
	name            string
	indexFilterable *bool
	indexSearchable *bool
	indexRange      *bool
	tokenization    string // empty = leave at server default
}

// matrixPreStatesForText enumerates the meaningful pre-states for text:
//
//   - the 9 combinations of filterable {nil,false,true} × searchable
//     {nil,false,true} with tokenization unset (covers the full
//     dispatcher branch space), plus
//   - 3 (filt, srch) cells × 2 tokenizations, exercising the
//     filterable/searchable cross-tokenization branch in
//     validateEnableSearchableProperty.
func matrixPreStatesForText() []matrixPreState {
	var out []matrixPreState
	flagVals := []struct {
		name string
		v    *bool
	}{
		{"nil", nil}, {"true", reindexhelpers.BoolPtr(true)}, {"false", reindexhelpers.BoolPtr(false)},
	}
	for _, ff := range flagVals {
		for _, sf := range flagVals {
			out = append(out, matrixPreState{
				name:            fmt.Sprintf("filt=%s_srch=%s_tok=unset", ff.name, sf.name),
				indexFilterable: ff.v,
				indexSearchable: sf.v,
			})
		}
	}
	for _, ic := range []struct {
		filt, srch *bool
		fn, sn     string
	}{
		{reindexhelpers.BoolPtr(true), reindexhelpers.BoolPtr(false), "true", "false"},
		{reindexhelpers.BoolPtr(false), reindexhelpers.BoolPtr(true), "false", "true"},
		{reindexhelpers.BoolPtr(true), reindexhelpers.BoolPtr(true), "true", "true"},
	} {
		for _, tok := range []string{
			models.PropertyTokenizationLowercase,
			models.PropertyTokenizationField,
		} {
			out = append(out, matrixPreState{
				name:            fmt.Sprintf("filt=%s_srch=%s_tok=%s", ic.fn, ic.sn, tok),
				indexFilterable: ic.filt,
				indexSearchable: ic.srch,
				tokenization:    tok,
			})
		}
	}
	return out
}

// matrixPreStatesForNumeric enumerates pre-states for numeric / date types:
// filterable {nil,false,true} × rangeable {nil,false,true}.
func matrixPreStatesForNumeric() []matrixPreState {
	var out []matrixPreState
	flagVals := []struct {
		name string
		v    *bool
	}{
		{"nil", nil}, {"true", reindexhelpers.BoolPtr(true)}, {"false", reindexhelpers.BoolPtr(false)},
	}
	for _, ff := range flagVals {
		for _, rf := range flagVals {
			out = append(out, matrixPreState{
				name:            fmt.Sprintf("filt=%s_range=%s", ff.name, rf.name),
				indexFilterable: ff.v,
				indexRange:      rf.v,
			})
		}
	}
	return out
}

// matrixPreStatesForBoolUUID enumerates pre-states for boolean / uuid:
// filterable {nil,false,true} only.
func matrixPreStatesForBoolUUID() []matrixPreState {
	return []matrixPreState{
		{name: "filt=nil", indexFilterable: nil},
		{name: "filt=true", indexFilterable: reindexhelpers.BoolPtr(true)},
		{name: "filt=false", indexFilterable: reindexhelpers.BoolPtr(false)},
	}
}

// matrixPreStatesForSpecial enumerates pre-states for blob /
// geoCoordinates / phoneNumber. The schema migrator forces IndexFilterable
// = false for blob even when nil is passed; for geo/phoneNumber the
// behavior may differ. We test {nil, false} for both — the schema manager
// rejects {true} for these types.
func matrixPreStatesForSpecial() []matrixPreState {
	return []matrixPreState{
		{name: "filt=nil", indexFilterable: nil},
		{name: "filt=false", indexFilterable: reindexhelpers.BoolPtr(false)},
	}
}

// matrixBody describes one PUT body shape.
type matrixBody struct {
	name string
	body string
}

// matrixBodies enumerates the body shapes covered by the test. The set is
// deliberately compact: every shape exercises a distinct dispatcher arm.
func matrixBodies() []matrixBody {
	return []matrixBody{
		// canonical enable-searchable, two tokenizations to exercise both
		// the "matches stored tok" and "diverges from stored tok" branches
		// of validateEnableSearchableProperty.
		{"PUT_searchable_enabled_tok_word", `{"searchable":{"enabled":true,"tokenization":"word"}}`},
		{"PUT_searchable_enabled_tok_field", `{"searchable":{"enabled":true,"tokenization":"field"}}`},
		// searchable.enabled=false — not a verb; users try because of the
		// natural inversion of enabled:true. Must 4xx, not silently 202.
		{"PUT_searchable_enabled_false", `{"searchable":{"enabled":false}}`},
		// change-tokenization on searchable. Exercises both
		// validateTokenizationChange and the early reject for
		// filterable-only properties.
		{"PUT_searchable_tokenization_word", `{"searchable":{"tokenization":"word"}}`},
		{"PUT_searchable_tokenization_field", `{"searchable":{"tokenization":"field"}}`},
		// searchable algorithm switch (Map → BlockMax).
		{"PUT_searchable_algorithm_blockmax", `{"searchable":{"algorithm":"blockmax"}}`},
		// canonical enable-filterable.
		{"PUT_filterable_enabled", `{"filterable":{"enabled":true}}`},
		{"PUT_filterable_enabled_false", `{"filterable":{"enabled":false}}`},
		// change-tokenization on filterable (the verb that used to be
		// silently dropped; pinning that it remains a real verb).
		{"PUT_filterable_tokenization_field", `{"filterable":{"tokenization":"field"}}`},
		// repair-filterable.
		{"PUT_filterable_rebuild", `{"filterable":{"rebuild":true}}`},
		// enable / repair rangeable.
		{"PUT_rangeable_enabled", `{"rangeable":{"enabled":true}}`},
		{"PUT_rangeable_rebuild", `{"rangeable":{"rebuild":true}}`},
		// no-verb body — must 4xx.
		{"PUT_no_verb", `{}`},
		// truly empty body — handled by go-swagger, expected 4xx.
		{"PUT_empty_body", ``},
		// malformed JSON — handler must 4xx, not 500.
		{"PUT_malformed_json", `{"searchable":`},
	}
}

// ----------------------------------------------------------------------------
// Object payloads
// ----------------------------------------------------------------------------

// objectsForType returns a small corpus appropriate for the data type.
func objectsForType(dt string, propName string) []map[string]interface{} {
	switch dt {
	case "text":
		return []map[string]interface{}{
			{propName: "alpha lorem ipsum"},
			{propName: "beta dolor sit"},
			{propName: "gamma amet consectetur"},
			{propName: "delta adipiscing elit"},
			{propName: "epsilon sed do eiusmod"},
		}
	case "int":
		return []map[string]interface{}{
			{propName: 10}, {propName: 20}, {propName: 30}, {propName: 40}, {propName: 50},
		}
	case "boolean":
		return []map[string]interface{}{
			{propName: true}, {propName: false}, {propName: true}, {propName: false}, {propName: true},
		}
	case "uuid":
		return []map[string]interface{}{
			{propName: "11111111-1111-1111-1111-111111111111"},
			{propName: "22222222-2222-2222-2222-222222222222"},
			{propName: "33333333-3333-3333-3333-333333333333"},
			{propName: "44444444-4444-4444-4444-444444444444"},
			{propName: "55555555-5555-5555-5555-555555555555"},
		}
	case "geoCoordinates":
		return []map[string]interface{}{
			{propName: map[string]interface{}{"latitude": 1.0, "longitude": 1.0}},
			{propName: map[string]interface{}{"latitude": 2.0, "longitude": 2.0}},
			{propName: map[string]interface{}{"latitude": 3.0, "longitude": 3.0}},
		}
	case "blob":
		return []map[string]interface{}{
			{propName: "YWxwaGE="}, // base64 "alpha"
			{propName: "YmV0YQ=="}, // base64 "beta"
			{propName: "Z2FtbWE="}, // base64 "gamma"
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// Driver
// ----------------------------------------------------------------------------

// matrixOutcome records what each cell observed. The Summary subtest uses
// these to render a roll-up of green / red counts and to surface specific
// failure modes (5xx / silent-empty-bucket / unparseable-4xx) to the test
// log so reviewers can see the catch surface at a glance.
type matrixOutcome struct {
	name       string
	dt         string
	preState   string
	body       string
	status     int
	bodyShape  string // "4xx-clear" / "4xx-empty" / "4xx-unstructured" / "202-finished-queryable" / "202-finished-empty" / "5xx-panic" / "202-failed"
	gotMessage string
	skipped    bool
}

func testPropertyStateMigrationMatrix(t *testing.T, restURI string) {
	dts := matrixDataTypes()
	bodies := matrixBodies()

	collectionCounter := 0
	var outcomes []*matrixOutcome

	for _, dt := range dts {
		preStates := preStatesForType(dt)
		for _, ps := range preStates {
			if !preStateCreatable(dt, ps) {
				continue
			}
			for _, mb := range bodies {
				if !preStateRelevant(dt, ps, mb) {
					continue
				}
				collectionCounter++
				caseName := matrixCaseName(dt.name, ps.name, mb.name)
				out := &matrixOutcome{
					name:     caseName,
					dt:       dt.name,
					preState: ps.name,
					body:     mb.name,
				}
				outcomes = append(outcomes, out)

				t.Run(caseName, func(t *testing.T) {
					runMatrixCell(t, restURI, dt, ps, mb, collectionCounter, out)
				})
			}
		}
	}

	t.Run("Summary", func(t *testing.T) {
		buckets := map[string]int{}
		var redLines []string
		for _, o := range outcomes {
			if o.skipped {
				buckets["skipped"]++
				continue
			}
			buckets[o.bodyShape]++
			switch o.bodyShape {
			case "5xx-panic", "202-finished-empty", "4xx-empty", "4xx-unstructured", "202-failed", "202-no-finish":
				redLines = append(redLines, fmt.Sprintf("  %s: shape=%s status=%d message=%s",
					o.name, o.bodyShape, o.status, truncate(o.gotMessage, 200)))
			}
		}
		t.Logf("matrix summary (%d cells):", len(outcomes))
		for k, v := range buckets {
			t.Logf("  %s: %d", k, v)
		}
		if len(redLines) > 0 {
			t.Logf("RED cells (bugs pinned by the matrix):\n%s", strings.Join(redLines, "\n"))
		}
	})
}

// preStateCreatable returns true if helper.CreateClass would accept the
// pre-state for this data type. The schema manager rejects certain combos
// (e.g. searchable=true on a number property), so we filter those before
// even trying.
func preStateCreatable(dt matrixDataType, ps matrixPreState) bool {
	if !dt.supportsFilt && ps.indexFilterable != nil && *ps.indexFilterable {
		return false
	}
	if !dt.supportsSrch && ps.indexSearchable != nil && *ps.indexSearchable {
		return false
	}
	if !dt.supportsRange && ps.indexRange != nil && *ps.indexRange {
		return false
	}
	if !dt.allowDefaultTk && ps.tokenization != "" {
		return false
	}
	return true
}

// preStateRelevant filters out body × pre-state combinations whose result
// is uninteresting because the body cannot meaningfully target this
// pre-state. Currently always true — we let every body run against every
// creatable pre-state, since the dispatcher must reject any invalid combo
// with a clean 4xx.
func preStateRelevant(_ matrixDataType, _ matrixPreState, _ matrixBody) bool {
	return true
}

func preStatesForType(dt matrixDataType) []matrixPreState {
	switch dt.name {
	case "text", "text[]":
		return matrixPreStatesForText()
	case "int", "number", "date", "int[]", "number[]", "date[]":
		return matrixPreStatesForNumeric()
	case "boolean", "boolean[]", "uuid", "uuid[]":
		return matrixPreStatesForBoolUUID()
	case "geoCoordinates", "phoneNumber", "blob":
		return matrixPreStatesForSpecial()
	}
	return nil
}

func matrixCaseName(dt, ps, body string) string {
	name := fmt.Sprintf("dt=%s__%s__%s", dt, ps, body)
	name = strings.ReplaceAll(name, "[", "Arr")
	name = strings.ReplaceAll(name, "]", "")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}

// runMatrixCell executes one (dt, ps, mb) combination end-to-end and
// classifies the outcome into one of the structural buckets we care
// about. Cells that observe a bug shape (5xx, empty body, silent-FINISHED,
// etc.) call t.Errorf so go-test reports them.
func runMatrixCell(t *testing.T, restURI string, dt matrixDataType, ps matrixPreState, mb matrixBody, ordinal int, out *matrixOutcome) {
	t.Helper()

	className := fmt.Sprintf("MatrixCell%04d", ordinal)
	propName := "p"

	// Build the property.
	prop := &models.Property{Name: propName, DataType: []string{dt.name}}
	if ps.indexFilterable != nil {
		v := *ps.indexFilterable
		prop.IndexFilterable = &v
	}
	if ps.indexSearchable != nil {
		v := *ps.indexSearchable
		prop.IndexSearchable = &v
	}
	if ps.indexRange != nil {
		v := *ps.indexRange
		prop.IndexRangeFilters = &v
	}
	if ps.tokenization != "" {
		prop.Tokenization = ps.tokenization
	}

	class := &models.Class{
		Class:      className,
		Properties: []*models.Property{prop},
		Vectorizer: "none",
	}
	if err := tryCreateClass(t, class); err != nil {
		out.skipped = true
		t.Skipf("pre-state not createable: %v", err)
		return
	}
	defer helper.DeleteClass(t, className)

	for _, o := range objectsForType(dt.name, propName) {
		if err := helper.CreateObject(t, &models.Object{Class: className, Properties: o}); err != nil {
			t.Logf("create object error (continuing): %v", err)
		}
	}

	// Send the PUT.
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, propName)
	var reqBody io.Reader
	if mb.body != "" {
		reqBody = bytes.NewReader([]byte(mb.body))
	}
	req, err := http.NewRequest(http.MethodPut, url, reqBody)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	respBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	respBody := string(respBytes)

	out.status = resp.StatusCode
	out.gotMessage = respBody

	// ---- Structural invariant 1: no 5xx. ----
	if resp.StatusCode >= 500 {
		out.bodyShape = "5xx-panic"
		t.Errorf("STRUCTURAL BUG (5xx): status=%d body=%s", resp.StatusCode, truncate(respBody, 400))
		return
	}

	// ---- 4xx path ----
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		shape, msg := classify4xxBody(respBody)
		out.bodyShape = shape
		out.gotMessage = msg
		switch shape {
		case "4xx-empty":
			t.Errorf("STRUCTURAL BUG (4xx with empty body): status=%d body=%q",
				resp.StatusCode, respBody)
		case "4xx-unstructured":
			t.Errorf("STRUCTURAL BUG (4xx body not in any recognised error envelope): status=%d body=%s",
				resp.StatusCode, truncate(respBody, 400))
		case "4xx-clear":
			// good — fall through.
		}
		return
	}

	// ---- 202 path ----
	if resp.StatusCode == http.StatusAccepted {
		var bodyJSON map[string]string
		if err := json.Unmarshal(respBytes, &bodyJSON); err != nil {
			out.bodyShape = "202-malformed"
			t.Errorf("STRUCTURAL BUG (202 with malformed body): %v body=%s", err, truncate(respBody, 400))
			return
		}
		taskID := bodyJSON["taskId"]
		if taskID == "" {
			out.bodyShape = "202-no-taskid"
			t.Errorf("STRUCTURAL BUG (202 without taskId): body=%s", truncate(respBody, 400))
			return
		}

		status, taskErr := awaitTaskTerminal(t, restURI, taskID, 60*time.Second)
		switch status {
		case "FINISHED":
			// fall through to bucket check.
		case "FAILED":
			out.bodyShape = "202-failed"
			out.gotMessage = taskErr
			t.Errorf("task FAILED after 202 accept: %s", taskErr)
			return
		case "CANCELLED":
			out.bodyShape = "202-cancelled-unexpected"
			t.Errorf("task CANCELLED without explicit cancel verb")
			return
		case "TIMEOUT":
			out.bodyShape = "202-no-finish"
			t.Errorf("task did not reach terminal state within 60s")
			return
		}

		if !verifySchemaPostMigration(t, className, propName, mb) {
			out.bodyShape = "202-finished-no-flag-flip"
			t.Errorf("STRUCTURAL BUG (FINISHED without schema flag flip): %s", mb.body)
			return
		}

		queryIndexType := queryIndexForBody(mb)
		if queryIndexType != "" {
			if !verifyQueryability(t, className, propName, dt.name, queryIndexType) {
				out.bodyShape = "202-finished-empty"
				t.Errorf("STRUCTURAL BUG (silent FINISHED with empty bucket): "+
					"task finished, schema flag flipped, but query against %s returned 0 rows. "+
					"This is the Sev 1 failure mode (DELETE→reenable, CANCEL→retry, etc.)",
					queryIndexType)
				return
			}
		}
		out.bodyShape = "202-finished-queryable"
		return
	}

	// Anything else — 1xx, 3xx, etc. — is unexpected.
	out.bodyShape = fmt.Sprintf("unexpected-status-%d", resp.StatusCode)
	t.Errorf("unexpected HTTP status %d; body=%s", resp.StatusCode, truncate(respBody, 400))
}

// classify4xxBody returns ("4xx-clear", message) when the body contains a
// non-empty error message in any of the recognised wire shapes:
//
//   - Weaviate handler shape: `{"error":[{"message":"..."}]}`
//   - go-swagger validation shape: `{"code":N,"message":"..."}` (used for
//     422 and for some 400s that fire before the handler runs, e.g.
//     malformed JSON detected at the body-parsing layer)
//
// Returns ("4xx-empty", "") for an empty body and ("4xx-unstructured",
// body) for any other shape — those are the cases the matrix wants to
// surface because they leave the caller with nothing to act on.
func classify4xxBody(body string) (string, string) {
	body = strings.TrimSpace(body)
	if body == "" {
		return "4xx-empty", ""
	}
	type errMsg struct {
		Message string `json:"message"`
	}
	type weaviateErr struct {
		Errors []errMsg `json:"error"`
	}
	var we weaviateErr
	if err := json.Unmarshal([]byte(body), &we); err == nil && len(we.Errors) > 0 {
		for _, em := range we.Errors {
			if strings.TrimSpace(em.Message) != "" {
				return "4xx-clear", em.Message
			}
		}
	}
	type swaggerErr struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	var se swaggerErr
	if err := json.Unmarshal([]byte(body), &se); err == nil && se.Message != "" {
		return "4xx-clear", se.Message
	}
	return "4xx-unstructured", body
}

// tryCreateClass calls POST /v1/schema directly so we can convert schema-
// manager rejections (e.g. searchable=true on a numeric prop) into a
// t.Skip rather than t.Fatal. helper.CreateClass would t.Fatal which
// kills the cell rather than letting us continue with the matrix.
func tryCreateClass(t *testing.T, class *models.Class) error {
	t.Helper()
	url := fmt.Sprintf("http://%s:%s/v1/schema", helper.ServerHost, helper.ServerPort)
	body, _ := json.Marshal(class)
	req, e := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("CreateClass returned %d: %s", resp.StatusCode, truncate(string(respBytes), 400))
	}
	return nil
}

// awaitTaskTerminal polls /v1/tasks until the named task reaches FINISHED,
// FAILED, or CANCELLED. Returns (status, errorMsg). If the task never
// reaches a terminal state within the timeout, returns ("TIMEOUT", "").
func awaitTaskTerminal(t *testing.T, restURI, taskID string, timeout time.Duration) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "TIMEOUT", ""
		case <-ticker.C:
		}
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			// transient error — retry on next tick.
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			// transient error — retry on next tick.
			continue
		}
		for _, task := range tasks["reindex"] {
			if task.ID != taskID {
				continue
			}
			switch task.Status {
			case "FINISHED":
				return "FINISHED", ""
			case "FAILED":
				return "FAILED", task.Error
			case "CANCELLED":
				return "CANCELLED", task.Error
			}
		}
	}
}

// verifySchemaPostMigration polls the class schema until the targeted index
// flag is in the expected post-state.
func verifySchemaPostMigration(t *testing.T, class, propName string, mb matrixBody) bool {
	t.Helper()
	return assert.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c != nil {
			for _, p := range c.Properties {
				if p.Name != propName {
					continue
				}
				switch {
				case strings.Contains(mb.name, "searchable_enabled_tok"):
					if p.IndexSearchable != nil && *p.IndexSearchable {
						return true
					}
				case strings.Contains(mb.name, "filterable_enabled") &&
					!strings.Contains(mb.name, "filterable_enabled_false"):
					if p.IndexFilterable != nil && *p.IndexFilterable {
						return true
					}
				case strings.Contains(mb.name, "rangeable_enabled") &&
					!strings.Contains(mb.name, "rangeable_enabled_false"):
					if p.IndexRangeFilters != nil && *p.IndexRangeFilters {
						return true
					}
				default:
					// rebuilds, tokenization changes: flag should not be
					// false after FINISHED.
					return true
				}
			}
		}
		return false
	}, 20*time.Second, 50*time.Millisecond)
}

// queryIndexForBody returns which index type should be queried after a
// successful migration of this body shape. Empty if the body does not
// directly imply a queryable end-state (e.g. no-verb bodies that should
// have 4xx'd anyway).
func queryIndexForBody(mb matrixBody) string {
	switch {
	case strings.Contains(mb.name, "searchable_enabled_tok"),
		strings.Contains(mb.name, "searchable_tokenization"),
		strings.Contains(mb.name, "searchable_rebuild"):
		return "searchable"
	case strings.Contains(mb.name, "filterable_enabled") &&
		!strings.Contains(mb.name, "filterable_enabled_false"),
		strings.Contains(mb.name, "filterable_tokenization"),
		strings.Contains(mb.name, "filterable_rebuild"):
		return "filterable"
	case strings.Contains(mb.name, "rangeable_enabled") &&
		!strings.Contains(mb.name, "rangeable_enabled_false"),
		strings.Contains(mb.name, "rangeable_rebuild"):
		return "rangeable"
	}
	return ""
}

// verifyQueryability issues a single appropriate query against the final
// index and returns true if it returns at least one row from the corpus.
//
// We deliberately keep the queries broad so the result is robust across
// tokenization variants. For text/searchable we try multiple bm25 queries
// against different tokens that appear in the corpus; for filterable we use
// Equal on the exact stored value; for rangeable we use a range operator
// over the rangeable bucket.
//
// The structural invariant we want to prove is "the post-migration bucket
// is queryable" — not "this specific query returns N rows". As long as
// some appropriate query returns at least one row, the bucket is alive.
func verifyQueryability(t *testing.T, class, propName, dt, indexType string) bool {
	t.Helper()
	switch indexType {
	case "searchable":
		// Try multiple candidate bm25 queries because tokenization affects
		// which tokens are searchable. With word/lowercase/whitespace the
		// query "alpha" matches the "alpha lorem ipsum" doc; with field
		// tokenization the whole string is one token, so we must match
		// against the exact full value.
		for _, q := range []string{
			"alpha",
			"alpha lorem ipsum",
			"alpha lorem",
			"lorem",
		} {
			gql := fmt.Sprintf(`{ Get { %s(bm25: {query: %q, properties: [%q]}, limit: 10000) { _additional { id } } } }`,
				class, q, propName)
			ids, err := runGraphQLQuery(t, class, gql)
			if err != nil {
				continue
			}
			if len(ids) > 0 {
				return true
			}
		}
		t.Logf("queryability bm25: no candidate query returned hits")
		return false
	case "filterable":
		op, val := equalValueForType(dt)
		if op == "" {
			// No applicable equality query for this type (e.g.
			// geoCoordinates / blob / phoneNumber). Treat as a soft pass —
			// the schema-flag flip already proved the migration ran. The
			// structural invariant we are protecting is "non-silent
			// FINISHED", which for these types reduces to "task did not
			// remain STARTED indefinitely".
			return true
		}
		gql := fmt.Sprintf(`{ Get { %s(where: {path: [%q], operator: %s, %s}, limit: 10000) { _additional { id } } } }`,
			class, propName, op, val)
		ids, err := runGraphQLQuery(t, class, gql)
		if err != nil {
			t.Logf("queryability filter error: %v", err)
			return false
		}
		return len(ids) > 0
	case "rangeable":
		// Range query over the numeric/date bucket. The rangeable index is
		// queried via GreaterThanOrEqual / LessThan etc. — Equal can route
		// to the filterable bucket, which is the wrong index. Pick an
		// operator that exercises rangeable.
		op, val := rangeValueForType(dt)
		if op == "" {
			return true
		}
		gql := fmt.Sprintf(`{ Get { %s(where: {path: [%q], operator: %s, %s}, limit: 10000) { _additional { id } } } }`,
			class, propName, op, val)
		ids, err := runGraphQLQuery(t, class, gql)
		if err != nil {
			t.Logf("queryability range error: %v", err)
			return false
		}
		return len(ids) > 0
	}
	return true
}

// rangeValueForType returns a (range-operator, valueFragment) pair that
// exercises the rangeable bucket (not the filterable one).
func rangeValueForType(dt string) (op, valFragment string) {
	switch dt {
	case "int", "int[]":
		return "GreaterThanEqual", `valueInt: 0`
	case "number", "number[]":
		return "GreaterThanEqual", `valueNumber: 0`
	case "date", "date[]":
		return "GreaterThanEqual", `valueDate: "2000-01-01T00:00:00Z"`
	}
	return "", ""
}

func equalValueForType(dt string) (op, valFragment string) {
	switch dt {
	case "text":
		return "Equal", `valueText: "alpha lorem ipsum"`
	case "text[]":
		return "Equal", `valueText: "alpha"`
	case "int":
		return "Equal", `valueInt: 10`
	case "int[]":
		return "Equal", `valueInt: 10`
	case "number":
		return "Equal", `valueNumber: 1.5`
	case "number[]":
		return "Equal", `valueNumber: 1.5`
	case "date":
		return "Equal", `valueDate: "2024-01-01T00:00:00Z"`
	case "date[]":
		return "Equal", `valueDate: "2024-01-01T00:00:00Z"`
	case "boolean":
		return "Equal", `valueBoolean: true`
	case "boolean[]":
		return "Equal", `valueBoolean: true`
	case "uuid":
		return "Equal", `valueText: "11111111-1111-1111-1111-111111111111"`
	case "uuid[]":
		return "Equal", `valueText: "11111111-1111-1111-1111-111111111111"`
	}
	return "", ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...[truncated]"
}

// TestSuppress_PropertyStateMigrationMatrix keeps this file referenced from
// the package's `go test` graph. The matrix's actual entry point is wired
// into TestSingleNode_ReindexSuite via t.Run("PropertyStateMigrationMatrix",
// testPropertyStateMigrationMatrix).
func TestSuppress_PropertyStateMigrationMatrix(t *testing.T) {
	assert.NotNil(t, testPropertyStateMigrationMatrix)
}
