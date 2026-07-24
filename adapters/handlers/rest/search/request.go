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

package search

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/filterext"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/configvalidation"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
)

// checkReservedFields rejects reserved (not yet supported) fields with 422.
// They are x-nullable in the spec so a non-nil pointer signals presence. Keep
// this set in lock-step with the reserved fields on SearchCommon in
// openapi-specs/schema.json.
func checkReservedFields(common *models.SearchCommon) *APIError {
	reserved := []struct {
		name    string
		present bool
	}{
		{"singlePrompt", common.SinglePrompt != nil},
		{"groupedTask", common.GroupedTask != nil},
		{"groupBy", common.GroupBy != nil},
		{"numberOfGroups", common.NumberOfGroups != nil},
		{"objectsPerGroup", common.ObjectsPerGroup != nil},
		{"rerank", common.Rerank != nil},
	}
	for _, r := range reserved {
		if r.present {
			return newAPIError(http.StatusUnprocessableEntity, "%s is not yet supported", r.name)
		}
	}
	return nil
}

// baseParams starts the dto.GetParams every search type shares: the
// collection, tenant, consistency level and pagination.
func (h *Handler) baseParams(className string, common *models.SearchCommon) (dto.GetParams, *APIError) {
	out := dto.GetParams{ClassName: className, Tenant: common.Tenant}

	replProps, apiErr := parseConsistencyLevel(common.ConsistencyLevel)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}
	out.ReplicationProperties = replProps

	pagination, apiErr := h.parsePagination(common)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}
	out.Pagination = pagination

	return out, nil
}

// fillSelectionAndFilter fills the tail every search type shares: the
// returnProperties selection (with the no-props marker) and the where filter.
func (h *Handler) fillSelectionAndFilter(out *dto.GetParams, class *models.Class, className string,
	common *models.SearchCommon, getClass classGetterFunc, principal *models.Principal,
) *APIError {
	props, apiErr := parseReturnProperties(class, common.ReturnProperties, getClass)
	if apiErr != nil {
		return apiErr
	}
	out.Properties = props
	if len(out.Properties) == 0 {
		out.AdditionalProperties.NoProps = true
	}

	filter, apiErr := parseWhere(common.Where, className, h.namespacesEnabled, principal, getClass)
	if apiErr != nil {
		return apiErr
	}
	out.Filters = filter

	return nil
}

// buildNearTextParams converts the near-text request into the dto.GetParams
// consumed by traverser.GetClass. Behavior must stay in sync with the gRPC
// parser (adapters/handlers/grpc/v1/parse_search_request.go). Shared fields
// are read from the embedded SearchCommon, near-text fields off the body.
func (h *Handler) buildNearTextParams(class *models.Class, className string, body *models.SearchNearTextRequest,
	getClass classGetterFunc, principal *models.Principal,
) (dto.GetParams, *APIError) {
	common := &body.SearchCommon
	out, apiErr := h.baseParams(className, common)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}

	targetVectors, apiErr := resolveTargetVectors(class, body.TargetVector)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}

	nearTextParams, apiErr := parseNearText(class, body, targetVectors, out.Pagination.Limit)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}
	out.ModuleParams = map[string]any{"nearText": nearTextParams}

	addProps, apiErr := parseReturnMetadata(class, common.ReturnMetadata, targetVectors)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}
	out.AdditionalProperties = addProps

	if apiErr := h.fillSelectionAndFilter(&out, class, className, common, getClass, principal); apiErr != nil {
		return dto.GetParams{}, apiErr
	}

	return out, nil
}

// buildBm25Params converts the bm25 request into the dto.GetParams consumed
// by traverser.GetClass. Behavior must stay in sync with the gRPC parser's
// bm25 handling (adapters/handlers/grpc/v1/parse_search_request.go). Shared
// fields are read from the embedded SearchCommon, bm25 fields off the body.
func (h *Handler) buildBm25Params(class *models.Class, className string, body *models.SearchBm25Request,
	getClass classGetterFunc, principal *models.Principal,
) (dto.GetParams, *APIError) {
	common := &body.SearchCommon
	out, apiErr := h.baseParams(className, common)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}

	addProps, apiErr := parseReturnMetadata(class, common.ReturnMetadata, nil)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}
	// certainty cannot be computed for a keyword search (gRPC parity)
	addProps.Certainty = false
	out.AdditionalProperties = addProps

	keywordRanking, apiErr := parseBm25(body, addProps.ExplainScore)
	if apiErr != nil {
		return dto.GetParams{}, apiErr
	}
	out.KeywordRanking = keywordRanking

	if apiErr := validateQueryProperties(class, body.QueryProperties); apiErr != nil {
		return dto.GetParams{}, apiErr
	}

	if apiErr := checkKeywordSearchable(class, body.QueryProperties); apiErr != nil {
		return dto.GetParams{}, apiErr
	}

	if apiErr := h.fillSelectionAndFilter(&out, class, className, common, getClass, principal); apiErr != nil {
		return dto.GetParams{}, apiErr
	}

	return out, nil
}

// validateQueryProperties rejects a queryProperties entry that names no
// schema property with 400, matching returnProperties. Whether an existing
// property is searchable stays the searcher's check (a typed
// MissingIndexError, mapped to 422). A "^boost" suffix is stripped before
// the lookup, mirroring the searcher.
func validateQueryProperties(class *models.Class, queryProperties []string) *APIError {
	for _, entry := range queryProperties {
		name := schema.LowercaseFirstLetter(strings.Split(entry, "^")[0])
		if _, err := schema.GetPropertyByName(class, name); err != nil {
			return &APIError{Status: http.StatusBadRequest, Err: err}
		}
	}
	return nil
}

// checkKeywordSearchable: empty queryProperties over a collection with no
// searchable property is a 422 here — the engine's all-properties expansion
// errors untyped (a 500). Explicit properties are the searcher's.
func checkKeywordSearchable(class *models.Class, queryProperties []string) *APIError {
	if len(queryProperties) > 0 {
		return nil
	}
	for _, prop := range class.Properties {
		if searchparams.PropertyHasSearchableIndex(class, prop.Name) {
			return nil
		}
	}
	return newAPIError(http.StatusUnprocessableEntity,
		"collection %s has no searchable properties for a keyword search", class.Class)
}

// parseBm25 builds the keyword-ranking params, mirroring the gRPC parser:
// first letter lowercased, "^boost" suffixes pass through to the searcher.
func parseBm25(body *models.SearchBm25Request, explainScore bool) (*searchparams.KeywordRanking, *APIError) {
	if body.Query == nil || *body.Query == "" {
		return nil, newAPIError(http.StatusBadRequest, "query must not be empty")
	}
	return &searchparams.KeywordRanking{
		Type:                   "bm25",
		Query:                  *body.Query,
		Properties:             schema.LowercaseFirstLetterOfStrings(body.QueryProperties),
		AdditionalExplanations: explainScore,
	}, nil
}

func parseConsistencyLevel(level string) (*additional.ReplicationProperties, *APIError) {
	if level == "" {
		return nil, nil
	}
	switch strings.ToUpper(level) {
	case "ONE", "QUORUM", "ALL":
		return &additional.ReplicationProperties{ConsistencyLevel: strings.ToUpper(level)}, nil
	default:
		return nil, newAPIError(http.StatusBadRequest,
			"consistencyLevel must be one of ONE, QUORUM, ALL, got %q", level)
	}
}

func (h *Handler) parsePagination(common *models.SearchCommon) (*filters.Pagination, *APIError) {
	pagination := &filters.Pagination{Limit: int(h.defaultLimit)}

	if common.Limit != nil {
		if *common.Limit < 0 {
			return nil, newAPIError(http.StatusBadRequest, "limit must not be negative, got %d", *common.Limit)
		}
		if *common.Limit > 0 {
			pagination.Limit = int(*common.Limit)
		}
	}
	if common.Offset != nil {
		if *common.Offset < 0 {
			return nil, newAPIError(http.StatusBadRequest, "offset must not be negative, got %d", *common.Offset)
		}
		pagination.Offset = int(*common.Offset)
	}
	if common.AutoLimit != nil {
		if *common.AutoLimit < 0 {
			return nil, newAPIError(http.StatusBadRequest, "autoLimit must not be negative, got %d", *common.AutoLimit)
		}
		pagination.Autocut = int(*common.AutoLimit)
	}

	// cap the page pre-db: a client error instead of a db 500, and no int
	// overflow into the negative special limit flags
	if h.maximumResults > 0 {
		if common.Limit != nil && *common.Limit > h.maximumResults {
			return nil, newAPIError(http.StatusBadRequest,
				"limit must not exceed QUERY_MAXIMUM_RESULTS (%d), got %d", h.maximumResults, *common.Limit)
		}
		if common.Offset != nil && *common.Offset > h.maximumResults {
			return nil, newAPIError(http.StatusBadRequest,
				"offset must not exceed QUERY_MAXIMUM_RESULTS (%d), got %d", h.maximumResults, *common.Offset)
		}
		if int64(pagination.Offset)+int64(pagination.Limit) > h.maximumResults {
			return nil, newAPIError(http.StatusBadRequest,
				"offset + limit must not exceed QUERY_MAXIMUM_RESULTS (%d), got %d",
				h.maximumResults, int64(pagination.Offset)+int64(pagination.Limit))
		}
	}

	return pagination, nil
}

// resolveTargetVectors resolves the target vector for the search: a
// collection with exactly one named vector selects it implicitly, a
// collection with several requires targetVector.
func resolveTargetVectors(class *models.Class, targetVector string) ([]string, *APIError) {
	var targetVectors []string
	if targetVector != "" {
		targetVectors = []string{targetVector}
	}

	if len(targetVectors) == 0 && !modelsext.ClassHasLegacyVectorIndex(class) {
		if len(class.VectorConfig) > 1 {
			return nil, newAPIError(http.StatusUnprocessableEntity,
				"collection %s has multiple vectors, but no target vectors were provided", class.Class)
		}
		for name := range class.VectorConfig {
			targetVectors = append(targetVectors, name)
		}
	}

	for _, target := range targetVectors {
		if _, ok := class.VectorConfig[target]; !ok {
			configuredNamedVectors := make([]string, 0, len(class.VectorConfig))
			for key := range class.VectorConfig {
				configuredNamedVectors = append(configuredNamedVectors, key)
			}
			return nil, newAPIError(http.StatusBadRequest,
				"collection %s does not have named vector %v configured. Available named vectors %v",
				class.Class, target, configuredNamedVectors)
		}
	}

	return targetVectors, nil
}

// parseNearText builds the nearText module params embedded server-side by
// the module pipeline.
func parseNearText(class *models.Class, body *models.SearchNearTextRequest, targetVectors []string, limit int) (*nearText.NearTextParams, *APIError) {
	values, apiErr := parseQuery(body.Query)
	if apiErr != nil {
		return nil, apiErr
	}

	if body.Certainty != nil && body.Distance != nil {
		return nil, newAPIError(http.StatusBadRequest, "near_text: cannot provide both distance and certainty")
	}
	if body.Certainty != nil && (*body.Certainty < 0 || *body.Certainty > 1) {
		return nil, newAPIError(http.StatusBadRequest,
			"certainty must be between 0 and 1, got %v", *body.Certainty)
	}

	params := &nearText.NearTextParams{
		Values:        values,
		Limit:         limit,
		TargetVectors: targetVectors,
	}
	if body.Certainty != nil {
		if err := configvalidation.CheckCertaintyCompatibility(class, targetVectors); err != nil {
			return nil, &APIError{Status: http.StatusUnprocessableEntity, Err: err}
		}
		params.Certainty = *body.Certainty
	}
	if body.Distance != nil {
		params.Distance = *body.Distance
		params.WithDistance = true
	}

	if apiErr := checkVectorizer(class, targetVectors, "near-text"); apiErr != nil {
		return nil, apiErr
	}

	return params, nil
}

// parseQuery validates the query concepts. `query` is an array of strings
// (Swagger 2.0 cannot express a string-or-array union, so a single concept is
// a one-element array); an absent query is already rejected upstream by
// swagger's required validation, leaving empty-array/empty-concept here.
func parseQuery(query []string) ([]string, *APIError) {
	if len(query) == 0 {
		return nil, newAPIError(http.StatusBadRequest, "query must not be empty")
	}
	for _, concept := range query {
		if concept == "" {
			return nil, newAPIError(http.StatusBadRequest, "query must not be empty")
		}
	}
	return query, nil
}

// checkVectorizer rejects searches that need server-side vectorization on
// collections whose (target) vector has no vectorizer module configured —
// deterministic counterpart of the modules provider's "could not vectorize
// input ..." runtime error. searchKind names the rejected search in the 422.
func checkVectorizer(class *models.Class, targetVectors []string, searchKind string) *APIError {
	noVectorizer := func(target string) *APIError {
		return newAPIError(http.StatusUnprocessableEntity,
			"%s is not supported: collection %s has no vectorizer module configured for target vector %q",
			searchKind, class.Class, target)
	}

	for _, target := range targetVectors {
		cfg, ok := class.VectorConfig[target]
		if !ok {
			continue // validated in resolveTargetVectors
		}
		// after a JSON/RAFT round-trip the vectorizer config is a
		// map[moduleName]interface{}; "none" means no vectorizer
		if vectorizer, ok := cfg.Vectorizer.(map[string]any); ok {
			if _, none := vectorizer["none"]; none {
				return noVectorizer(target)
			}
		}
	}

	if len(targetVectors) == 0 && (class.Vectorizer == "" || class.Vectorizer == "none") {
		return noVectorizer("")
	}

	return nil
}

// parseReturnMetadata converts returnMetadata into additional.Properties.
// returnMetadata selects metadata keys only; the object id is not one of
// them — it is always requested internally (additional.Properties{ID: true})
// and returned as each result's id field, whatever the list contains.
func parseReturnMetadata(class *models.Class, returnMetadata []string, targetVectors []string) (additional.Properties, *APIError) {
	props := additional.Properties{ID: true}
	for _, entry := range returnMetadata {
		switch entry {
		case "distance":
			props.Distance = true
		case "certainty":
			props.Certainty = true
		case "score":
			props.Score = true
		case "explainScore":
			props.ExplainScore = true
		case "creationTime":
			props.CreationTimeUnix = true
		case "lastUpdateTime":
			props.LastUpdateTimeUnix = true
		default:
			return additional.Properties{}, newAPIError(http.StatusBadRequest,
				"unknown returnMetadata entry %q, expected one of distance, certainty, score, explainScore, creationTime, lastUpdateTime", entry)
		}
	}

	// certainty is not compatible with non-cosine indexes; drop it silently
	if props.Certainty && configvalidation.CheckCertaintyCompatibility(class, targetVectors) != nil {
		props.Certainty = false
	}

	return props, nil
}

// parseReturnProperties builds the property selection. nil selects all
// non-ref, non-blob properties; dot-paths ("hasAuthor.name") select one hop
// across a reference; a bare reference name selects all non-ref properties
// of the referenced collection.
func parseReturnProperties(class *models.Class, returnProperties []string,
	getClass classGetterFunc,
) (search.SelectProperties, *APIError) {
	if returnProperties == nil {
		props, err := search.AllNonRefNonBlobProperties(class)
		if err != nil {
			return nil, &APIError{Status: http.StatusBadRequest, Err: err}
		}
		return props, nil
	}

	props := make(search.SelectProperties, 0, len(returnProperties))
	refSelections := map[string]*search.SelectProperty{}

	for _, entry := range returnProperties {
		if entry == "" {
			return nil, newAPIError(http.StatusBadRequest, "returnProperties entries must not be empty")
		}

		root, sub, isDotPath := strings.Cut(entry, ".")
		normalized := schema.LowercaseFirstLetter(root)

		schemaProp, err := schema.GetPropertyByName(class, normalized)
		if err != nil {
			return nil, &APIError{Status: http.StatusBadRequest, Err: err}
		}

		if !schema.IsRefDataType(schemaProp.DataType) {
			if isDotPath {
				return nil, newAPIError(http.StatusBadRequest,
					"returnProperties: %q is not a reference property, dot-paths only select across references", root)
			}
			prop, apiErr := nonRefSelectProperty(schemaProp, normalized)
			if apiErr != nil {
				return nil, apiErr
			}
			props = append(props, *prop)
			continue
		}

		refProp, apiErr := refSelectProperty(schemaProp, normalized, sub, isDotPath, refSelections, getClass)
		if apiErr != nil {
			return nil, apiErr
		}
		if refProp != nil {
			props = append(props, *refProp)
		}
	}

	// materialize merged ref selections in request order
	for i := range props {
		if merged, ok := refSelections[props[i].Name]; ok {
			props[i] = *merged
		}
	}

	return props, nil
}

func nonRefSelectProperty(schemaProp *models.Property, name string) (*search.SelectProperty, *APIError) {
	if isNestedDataType(schemaProp.DataType) {
		nestedProps, err := search.AllNonRefNonBlobNestedProperties(&property{schemaProp})
		if err != nil {
			return nil, &APIError{Status: http.StatusBadRequest, Err: err}
		}
		return &search.SelectProperty{
			Name:     name,
			IsObject: true,
			Props:    nestedProps,
		}, nil
	}

	return &search.SelectProperty{Name: name, IsPrimitive: true}, nil
}

// refSelectProperty resolves a one-hop reference selection. Multiple entries
// sharing the same root ("hasAuthor.name", "hasAuthor.age") merge into a
// single selection; the first occurrence claims the slot in the output, so a
// nil, nil return means "already emitted". Deeper hops are deferred.
func refSelectProperty(schemaProp *models.Property, name, sub string, isDotPath bool,
	refSelections map[string]*search.SelectProperty,
	getClass classGetterFunc,
) (*search.SelectProperty, *APIError) {
	if isDotPath && strings.Contains(sub, ".") {
		return nil, newAPIError(http.StatusUnprocessableEntity,
			"returnProperties: %q is not yet supported, only one reference hop is supported (e.g. %s.%s)",
			name+"."+sub, name, strings.Split(sub, ".")[0])
	}
	if len(schemaProp.DataType) != 1 {
		return nil, newAPIError(http.StatusUnprocessableEntity,
			"returnProperties: multi-target reference %q is not yet supported", name)
	}

	linkedClassName := schemaProp.DataType[0]
	linkedClass, err := getClass(linkedClassName)
	if err != nil {
		return nil, statusFromError(err)
	}

	existing, seen := refSelections[name]
	if !seen {
		existing = &search.SelectProperty{
			Name: name,
			Refs: []search.SelectClass{{ClassName: linkedClassName}},
		}
		refSelections[name] = existing
	}

	if !isDotPath {
		// bare reference name: all non-ref properties of the target
		refProps, err := search.AllNonRefNonBlobProperties(linkedClass)
		if err != nil {
			return nil, &APIError{Status: http.StatusBadRequest, Err: err}
		}
		existing.Refs[0].RefProperties = append(existing.Refs[0].RefProperties, refProps...)
	} else {
		subNormalized := schema.LowercaseFirstLetter(sub)
		subProp, err := schema.GetPropertyByName(linkedClass, subNormalized)
		if err != nil {
			return nil, &APIError{Status: http.StatusBadRequest, Err: err}
		}
		if schema.IsRefDataType(subProp.DataType) {
			return nil, newAPIError(http.StatusUnprocessableEntity,
				"returnProperties: %q is not yet supported, only one reference hop is supported", name+"."+sub)
		}
		selectProp, apiErr := nonRefSelectProperty(subProp, subNormalized)
		if apiErr != nil {
			return nil, apiErr
		}
		existing.Refs[0].RefProperties = append(existing.Refs[0].RefProperties, *selectProp)
	}

	if seen {
		return nil, nil
	}
	return existing, nil
}

func parseWhere(where *models.WhereFilter, className string, namespacesEnabled bool,
	principal *models.Principal, getClass classGetterFunc,
) (*filters.LocalFilter, *APIError) {
	if where == nil {
		return nil, nil
	}

	filter, err := filterext.Parse(where, className, namespacesEnabled, principal)
	if err != nil {
		return nil, &APIError{Status: http.StatusBadRequest, Err: err}
	}

	if err := filters.ValidateFilters(getClass, filter); err != nil {
		apiErr := statusFromError(err)
		if apiErr.Status == http.StatusInternalServerError {
			apiErr = &APIError{Status: http.StatusBadRequest, Err: fmt.Errorf("invalid 'where' filter: %w", err)}
		}
		return nil, apiErr
	}

	return filter, nil
}

// property adapts *models.Property to schema.PropertyInterface for the
// shared nested-property selection.
type property struct {
	*models.Property
}

func (p *property) GetName() string {
	return p.Name
}

func (p *property) GetNestedProperties() []*models.NestedProperty {
	return p.NestedProperties
}

func isNestedDataType(dataType []string) bool {
	return len(dataType) == 1 && schema.IsNested(schema.DataType(dataType[0]))
}
