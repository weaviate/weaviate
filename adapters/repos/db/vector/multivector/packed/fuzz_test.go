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

package packed

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzParse checks the two properties Parse owes its callers on arbitrary
// bytes: it never panics, and a Blob it returns is internally consistent, so
// the accessors are safe for every token the header claims.
//
// Parse reads values straight off disk, where a torn write or a truncated
// segment can hand it anything. TestParseRejects covers hand-written
// corruptions; this covers arbitrary ones.
//
// Without -fuzz this runs as an ordinary test over the seed corpus. To fuzz:
//
//	go test ./adapters/repos/db/vector/multivector/packed/ -run FuzzParse -fuzz FuzzParse
func FuzzParse(f *testing.F) {
	// seed with the committed fixtures and a few hand-built blobs, so the
	// fuzzer starts from inputs that parse and mutates outwards
	for _, name := range []string{
		"v1_float32.blob", "v1_float32_scalars.blob",
		"v1_rq1.blob", "v1_rq1_centered.blob", "v1_rq1_block16.blob",
	} {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			f.Fatalf("read %s: %v", name, err)
		}
		f.Add(b)
	}
	for _, n := range []int{0, 1, 32} {
		blob, err := EncodeFloat32(fixedTokens(n, 8), 8)
		if err != nil {
			f.Fatalf("EncodeFloat32: %v", err)
		}
		f.Add(blob)
	}
	f.Add([]byte{})
	f.Add([]byte(magic))

	f.Fuzz(func(t *testing.T, data []byte) {
		parsed, err := Parse(data)
		if err != nil {
			return
		}

		// the sections must exactly cover the input beyond the header, with
		// nothing left over and nothing overlapping
		if got := headerLenV1 + len(parsed.Codes()) + len(parsed.Scalars()); got != len(data) {
			t.Fatalf("sections cover %d bytes of a %d byte blob", got, len(data))
		}

		perToken, err := codeLen(parsed.Encoding, parsed.Dims)
		if err != nil {
			t.Fatalf("Parse accepted encoding %d that codeLen rejects: %v", parsed.Encoding, err)
		}
		scalarSize, err := parsed.ScalarKind.size()
		if err != nil {
			t.Fatalf("Parse accepted scalar kind %d that size rejects: %v", parsed.ScalarKind, err)
		}
		if want := int(parsed.Tokens) * perToken; len(parsed.Codes()) != want {
			t.Fatalf("code section is %d bytes, want %d", len(parsed.Codes()), want)
		}
		if want := int(parsed.Tokens) * scalarSize; len(parsed.Scalars()) != want {
			t.Fatalf("scalar section is %d bytes, want %d", len(parsed.Scalars()), want)
		}

		// Every token the header claims must be reachable. This loop cannot
		// run away: a successful parse needs Tokens*perToken code bytes to be
		// present and perToken is at least 1, so Tokens is at most len(data).
		for i := range int(parsed.Tokens) {
			if got := len(parsed.Code(i)); got != perToken {
				t.Fatalf("Code(%d) is %d bytes, want %d", i, got, perToken)
			}
			parsed.Scalar(i)
		}

		// decoding is only defined for exactly what EncodeFloat32 writes
		// (encoding, layout, no scalar section); anything else must be refused
		if parsed.Encoding == EncodingFloat32 && parsed.Layout == LayoutTokenMajor &&
			parsed.ScalarKind == ScalarNone {
			tokens, err := DecodeFloat32(parsed)
			if err != nil {
				t.Fatalf("DecodeFloat32 rejected a blob Parse accepted: %v", err)
			}
			if len(tokens) != int(parsed.Tokens) {
				t.Fatalf("decoded %d tokens, header claims %d", len(tokens), parsed.Tokens)
			}
		} else if _, err := DecodeFloat32(parsed); err == nil {
			t.Fatal("DecodeFloat32 accepted an encoding or layout it does not implement")
		}
	})
}
