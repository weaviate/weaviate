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

// Command openapi_proto_check guards the hand-curated REST query/aggregate
// OpenAPI definitions against drift from the gRPC proto. For each watched proto
// message it compares the proto's fields (via protoreflect on the compiled pb
// types) to the corresponding OpenAPI definition in openapi-specs/schema.json
// and fails (exit 1) when they disagree:
//
//   - a NON-deprecated proto field is MISSING from the OpenAPI def
//     (i.e. a field was added to the proto but never documented), and
//   - an OpenAPI field is NOT a proto field at all (renamed/removed in the
//     proto, or a typo), excluding intentional REST-only conveniences.
//
// Deprecated proto fields that are undocumented are allowed (we intentionally
// don't surface them). Run from the repo root:
//
//	go run ./tools/openapi_proto_check [path/to/schema.json]
//
// Wire it into CI next to the swagger codegen check so new proto fields can't
// land without the REST docs being updated (or explicitly allow-listed below).
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// force registration of the weaviate.v1 proto files into the global registry.
var _ = pb.SearchRequest{}

type watch struct {
	def      string   // OpenAPI definition name that mirrors this proto message
	restOnly []string // OpenAPI fields intentionally added by REST (not in the proto)
	omit     []string // non-deprecated proto fields intentionally left undocumented
}

// watched lists the request/search proto messages whose shape the REST docs
// must keep up with. Reply messages are intentionally excluded (modeled loosely
// on purpose). Add a message here when it gains a documented OpenAPI def.
var watched = map[string]watch{
	"weaviate.v1.SearchRequest":     {def: "SearchRequest", restOnly: []string{"where"}},
	"weaviate.v1.AggregateRequest":  {def: "AggregateRequest", restOnly: []string{"where"}},
	"weaviate.v1.NearVector":        {def: "NearVector"},
	"weaviate.v1.NearTextSearch":    {def: "NearTextSearch"},
	"weaviate.v1.NearObject":        {def: "NearObject"},
	"weaviate.v1.NearImageSearch":   {def: "NearImageSearch"},
	"weaviate.v1.NearAudioSearch":   {def: "NearAudioSearch"},
	"weaviate.v1.NearVideoSearch":   {def: "NearVideoSearch"},
	"weaviate.v1.NearDepthSearch":   {def: "NearDepthSearch"},
	"weaviate.v1.NearThermalSearch": {def: "NearThermalSearch"},
	"weaviate.v1.NearIMUSearch":     {def: "NearIMUSearch"},
	"weaviate.v1.BM25":              {def: "BM25"},
	"weaviate.v1.Hybrid":            {def: "Hybrid"},
	"weaviate.v1.Filters":           {def: "Filters"},
	"weaviate.v1.MetadataRequest":   {def: "MetadataRequest"},
	"weaviate.v1.PropertiesRequest": {def: "PropertiesRequest"},
	"weaviate.v1.GroupBy":           {def: "SearchGroupBy"},
	"weaviate.v1.SortBy":            {def: "SortBy"},
	"weaviate.v1.Rerank":            {def: "Rerank"},
	"weaviate.v1.Targets":           {def: "Targets"},

	"weaviate.v1.Boost":                       {def: "Boost"},
	"weaviate.v1.Boost.Condition":             {def: "BoostCondition"},
	"weaviate.v1.Boost.PropertyValueFunction": {def: "BoostPropertyValueFunction"},
	"weaviate.v1.Boost.TimeDecayFunction":     {def: "BoostTimeDecayFunction"},
	"weaviate.v1.Boost.NumericDecayFunction":  {def: "BoostNumericDecayFunction"},
}

type spec struct {
	Definitions map[string]struct {
		Properties map[string]json.RawMessage `json:"properties"`
	} `json:"definitions"`
}

func deprecated(fd protoreflect.FieldDescriptor) bool {
	if o, ok := fd.Options().(*descriptorpb.FieldOptions); ok {
		return o.GetDeprecated()
	}
	return false
}

func set(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func main() {
	path := "openapi-specs/schema.json"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
		os.Exit(2)
	}
	var s spec
	if err := json.Unmarshal(raw, &s); err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
		os.Exit(2)
	}

	fulls := make([]string, 0, len(watched))
	for k := range watched {
		fulls = append(fulls, k)
	}
	sort.Strings(fulls)

	var problems []string
	for _, full := range fulls {
		w := watched[full]
		dsc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(full))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: proto message not found in registry: %v", full, err))
			continue
		}
		md, ok := dsc.(protoreflect.MessageDescriptor)
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: registry entry is not a message", full))
			continue
		}
		def, ok := s.Definitions[w.def]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: OpenAPI definition %q is missing", full, w.def))
			continue
		}

		documented := make(map[string]bool, len(def.Properties))
		for k := range def.Properties {
			documented[k] = true
		}
		restOnly, omit := set(w.restOnly), set(w.omit)

		// proto -> OpenAPI: every non-deprecated proto field must be documented.
		protoFields := make(map[string]bool)
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			jn := fd.JSONName()
			protoFields[jn] = true
			if deprecated(fd) || omit[jn] || documented[jn] {
				continue
			}
			problems = append(problems, fmt.Sprintf(
				"%s.%s: proto field is MISSING from OpenAPI def %q (document it, add to omit list, or mark deprecated in the proto)",
				full, jn, w.def))
		}

		// OpenAPI -> proto: every documented field must be a real proto field or a REST-only convenience.
		extras := make([]string, 0)
		for k := range documented {
			if !protoFields[k] && !restOnly[k] {
				extras = append(extras, k)
			}
		}
		sort.Strings(extras)
		for _, k := range extras {
			problems = append(problems, fmt.Sprintf(
				"%s: OpenAPI def %q has field %q that is not in the proto (renamed/removed? typo? or add to restOnly)",
				full, w.def, k))
		}
	}

	if len(problems) == 0 {
		fmt.Printf("OpenAPI ↔ proto: in sync (%d messages checked)\n", len(watched))
		return
	}
	fmt.Fprintf(os.Stderr, "OpenAPI ↔ proto drift (%d):\n", len(problems))
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, "  - "+p)
	}
	os.Exit(1)
}
