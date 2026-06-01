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

// Command swagger_strip_doc_only reads an OpenAPI 2.0 spec and writes a copy
// with every entry marked `"x-doc-only": true` removed — definitions, path
// operations, and tags.
//
// These entries are kept in openapi-specs/schema.json so the documentation site
// can render them, but they must NOT reach go-swagger code generation: they
// describe endpoints (the REST query/aggregate endpoints) that are served by
// custom middleware over the gRPC pipeline, not by generated handlers, and the
// "swagger" CI job both regenerates from the spec and fails on any diff. Feeding
// swagger this stripped copy keeps the generated/committed code unchanged.
//
// Usage: swagger_strip_doc_only <input-spec> <output-spec>
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// httpMethods are the OpenAPI 2.0 path-item keys that denote an operation.
var httpMethods = []string{"get", "put", "post", "delete", "options", "head", "patch", "trace"}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: swagger_strip_doc_only <input-spec> <output-spec>")
		os.Exit(2)
	}
	in, out := os.Args[1], os.Args[2]

	raw, err := os.ReadFile(in)
	if err != nil {
		fatalf("read %s: %v", in, err)
	}

	// UseNumber keeps numeric literals byte-exact so the stripped spec the
	// generator sees is otherwise identical to the input.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var spec map[string]any
	if err := dec.Decode(&spec); err != nil {
		fatalf("parse %s: %v", in, err)
	}

	stripDefinitions(spec)
	stripPaths(spec)
	stripTags(spec)

	enc, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(out, append(enc, '\n'), 0o644); err != nil {
		fatalf("write %s: %v", out, err)
	}
}

// docOnly reports whether the given spec node carries `"x-doc-only": true`.
func docOnly(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	b, _ := m["x-doc-only"].(bool)
	return b
}

func stripDefinitions(spec map[string]any) {
	defs, ok := spec["definitions"].(map[string]any)
	if !ok {
		return
	}
	for name, def := range defs {
		if docOnly(def) {
			delete(defs, name)
		}
	}
}

func stripPaths(spec map[string]any) {
	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		return
	}
	for p, item := range paths {
		pi, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if docOnly(pi) {
			delete(paths, p)
			continue
		}
		hasOp := false
		for _, method := range httpMethods {
			op, exists := pi[method]
			if !exists {
				continue
			}
			if docOnly(op) {
				delete(pi, method)
				continue
			}
			hasOp = true
		}
		// Drop a path item that has no operations left.
		if !hasOp {
			delete(paths, p)
		}
	}
}

func stripTags(spec map[string]any) {
	tags, ok := spec["tags"].([]any)
	if !ok {
		return
	}
	kept := make([]any, 0, len(tags))
	for _, t := range tags {
		if docOnly(t) {
			continue
		}
		kept = append(kept, t)
	}
	spec["tags"] = kept
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
