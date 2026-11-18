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
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden blobs under testdata/")

// float32Codes packs tokens into a float32 code section: every coordinate as a
// little-endian float32, tokens in order.
func float32Codes(tokens [][]float32) []byte {
	var codes []byte
	for _, t := range tokens {
		for _, v := range t {
			codes = binary.LittleEndian.AppendUint32(codes, math.Float32bits(v))
		}
	}
	return codes
}

// fixedTokens returns n tokens of dims coordinates with reproducible,
// sign-varied values in [-1, 1], the magnitudes unit-norm token vectors have.
// Token 1 is all zeros: real multi-vectors contain zero tokens, and they must
// encode like any other token.
func fixedTokens(n, dims int) [][]float32 {
	tokens := make([][]float32, n)
	for i := range tokens {
		t := make([]float32, dims)
		if i != 1 {
			for j := range t {
				t[j] = float32((i*31+j*17)%2001-1000) / 1000
			}
		}
		tokens[i] = t
	}
	return tokens
}

// TestBuildParseRoundTrip builds blobs of many shapes and checks that Parse
// returns the same header, the same code section and the same scalars.
func TestBuildParseRoundTrip(t *testing.T) {
	// token counts cover the degenerate ends (0 and 1) and a range of
	// realistic document lengths
	for _, dims := range []int{128, 256, 1024} {
		for _, n := range []int{0, 1, 32, 124, 180, 1030} {
			for _, kind := range []ScalarKind{ScalarNone, ScalarFloat16} {
				name := fmt.Sprintf("d%d_n%d_%s", dims, n, scalarKindName(kind))
				t.Run(name, func(t *testing.T) {
					tokens := fixedTokens(n, dims)

					var scalars []float32
					if kind == ScalarFloat16 {
						scalars = make([]float32, n)
						for i := range scalars {
							// sixteenths, so every value is exactly
							// representable in binary16 and the round trip
							// below can be compared exactly; rounding is
							// covered by TestScalarRounding
							scalars[i] = float32(i%17)/16 - 0.5
						}
					}

					blob, err := build(Header{
						Encoding:         EncodingFloat32,
						Layout:           LayoutTokenMajor,
						ScalarKind:       kind,
						Dims:             uint16(dims),
						Tokens:           uint32(n),
						QuantizerID:      7,
						QuantizerVersion: 3,
					}, scalars, float32Codes(tokens))
					if err != nil {
						t.Fatalf("build: %v", err)
					}

					parsed, err := Parse(blob)
					if err != nil {
						t.Fatalf("Parse: %v", err)
					}
					want := Header{
						Version:          Version,
						Encoding:         EncodingFloat32,
						Layout:           LayoutTokenMajor,
						ScalarKind:       kind,
						Dims:             uint16(dims),
						Tokens:           uint32(n),
						QuantizerID:      7,
						QuantizerVersion: 3,
					}
					if parsed.Header != want {
						t.Fatalf("header = %+v, want %+v", parsed.Header, want)
					}
					if !bytes.Equal(parsed.Codes(), float32Codes(tokens)) {
						t.Fatal("code section does not match what was written")
					}
					// the scalars above are exactly representable in binary16,
					// so this comparison is exact despite the narrowing
					for i := range n {
						got := parsed.Scalar(i)
						want := float32(1)
						if kind == ScalarFloat16 {
							want = scalars[i]
						}
						if got != want {
							t.Fatalf("Scalar(%d) = %v, want %v", i, got, want)
						}
					}
				})
			}
		}
	}
}

// scalarKindName names k for a subtest.
func scalarKindName(k ScalarKind) string {
	if k == ScalarFloat16 {
		return "f16scalars"
	}
	return "noscalars"
}

// TestBuildRejects checks that build refuses headers and sections that do not
// describe a valid blob.
func TestBuildRejects(t *testing.T) {
	tests := []struct {
		name    string
		header  Header
		scalars []float32
		codes   []byte
	}{
		{
			name:   "zero dimensions",
			header: Header{Encoding: EncodingFloat32, Layout: LayoutTokenMajor, Dims: 0, Tokens: 1},
			codes:  make([]byte, 4),
		},
		{
			name:   "unknown encoding",
			header: Header{Encoding: 99, Layout: LayoutTokenMajor, Dims: 4, Tokens: 1},
			codes:  make([]byte, 16),
		},
		{
			// id 3 is not assigned (1 and 2 are float32 and RQ1), so it must
			// be rejected and not silently sized as something else
			name:   "unassigned encoding",
			header: Header{Encoding: 3, Layout: LayoutTokenMajor, Dims: 4, Tokens: 1},
			codes:  make([]byte, 16),
		},
		{
			name:   "unknown scalar kind",
			header: Header{Encoding: EncodingFloat32, ScalarKind: 99, Dims: 4, Tokens: 1},
			codes:  make([]byte, 16),
		},
		{
			name:   "missing scalars",
			header: Header{Encoding: EncodingFloat32, ScalarKind: ScalarFloat16, Dims: 4, Tokens: 2},
			codes:  make([]byte, 32),
		},
		{
			name:    "scalars without a scalar section",
			header:  Header{Encoding: EncodingFloat32, ScalarKind: ScalarNone, Dims: 4, Tokens: 2},
			scalars: []float32{1, 2},
			codes:   make([]byte, 32),
		},
		{
			name:   "short code section",
			header: Header{Encoding: EncodingFloat32, Dims: 4, Tokens: 2},
			codes:  make([]byte, 31),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := build(tt.header, tt.scalars, tt.codes); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestParseRejects corrupts a valid blob in one way per case and checks that
// Parse refuses it, wrapping the sentinel error where one is defined.
func TestParseRejects(t *testing.T) {
	valid, err := EncodeFloat32(fixedTokens(3, 4), 4)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}

	// wantErr is set where a caller must be able to tell the case apart: not
	// a packed blob, a packed blob from a newer build, a packed blob with an
	// encoding this build does not implement. Everything else is plain
	// corruption and only has to fail.
	tests := []struct {
		name    string
		corrupt func(b []byte) []byte
		wantErr error
	}{
		{name: "empty", corrupt: func([]byte) []byte { return nil }},
		{name: "shorter than the magic", corrupt: func(b []byte) []byte { return b[:3] }},
		{name: "too short for headerLen", corrupt: func(b []byte) []byte { return b[:minParseLen-1] }},
		{"bad magic", func(b []byte) []byte { b[0] = 'X'; return b }, ErrBadMagic},
		{"version zero", func(b []byte) []byte { b[offVersion] = 0; return b }, ErrUnsupportedVersion},
		{"version from the future", func(b []byte) []byte { b[offVersion] = Version + 1; return b }, ErrUnsupportedVersion},
		{name: "headerLen below the version's", corrupt: func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[offHeaderLen:], headerLenV1-1)
			return b
		}},
		{name: "headerLen past the end", corrupt: func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[offHeaderLen:], uint16(len(b)+1))
			return b
		}},
		{
			// The case that makes checking headerLen against the version
			// necessary. The valid blob is 24 + 3*16 = 72 bytes; claiming a
			// 40-byte header and 2 tokens gives 40 + 2*16 = 72 as well, so
			// every size check still passes and a reader that trusted the
			// field would return 32 bytes from the wrong offset.
			name: "headerLen inflated but the totals still add up",
			corrupt: func(b []byte) []byte {
				binary.LittleEndian.PutUint16(b[offHeaderLen:], 40)
				binary.LittleEndian.PutUint32(b[offTokens:], 2)
				return b
			},
		},
		{"unknown encoding", func(b []byte) []byte { b[offEncoding] = 99; return b }, ErrUnknownEncoding},
		{"unassigned encoding", func(b []byte) []byte { b[offEncoding] = 3; return b }, ErrUnknownEncoding},
		{"unknown scalar kind", func(b []byte) []byte { b[offScalarKind] = 99; return b }, ErrUnknownEncoding},
		{name: "zero dimensions", corrupt: func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[offDims:], 0)
			return b
		}},
		{name: "token count disagrees with the payload", corrupt: func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[offTokens:], 4)
			return b
		}},
		{
			// 2^30+1 tokens of 4 bytes is 2^32+4 bytes, which wraps to 4 in a
			// 32-bit int, so a 4-byte payload would pass a size check done in
			// int. It cannot pass on a 64-bit build even without the uint64
			// sum, so this case only fails on 32-bit builds.
			name: "token count that wraps 32-bit size math",
			corrupt: func(b []byte) []byte {
				binary.LittleEndian.PutUint16(b[offDims:], 1)
				binary.LittleEndian.PutUint32(b[offTokens:], 1<<30+1)
				return b[:headerLenV1+4]
			},
		},
		{name: "dims disagree with the payload", corrupt: func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[offDims:], 8)
			return b
		}},
		{name: "scalar section appears without room for it", corrupt: func(b []byte) []byte {
			b[offScalarKind] = uint8(ScalarFloat16)
			return b
		}},
		{name: "truncated payload", corrupt: func(b []byte) []byte { return b[:len(b)-1] }},
		{name: "trailing bytes", corrupt: func(b []byte) []byte { return append(b, 0) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.corrupt(bytes.Clone(valid)))
			if err == nil {
				t.Fatal("expected an error, got none")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want it to wrap %v", err, tt.wantErr)
			}
		})
	}
}

// TestReservedBytesIgnored pins the reserved bytes' contract: written zero and
// not read. That is what lets a later version use them without breaking blobs
// already written, and what keeps a reader from rejecting a blob over bytes it
// has no meaning for.
func TestReservedBytesIgnored(t *testing.T) {
	tokens := fixedTokens(3, 4)
	blob, err := EncodeFloat32(tokens, 4)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}
	if got := binary.LittleEndian.Uint32(blob[offReserved:]); got != 0 {
		t.Fatalf("reserved bytes written as %#x, want 0", got)
	}
	binary.LittleEndian.PutUint32(blob[offReserved:], 0xdeadbeef)
	parsed, err := Parse(blob)
	if err != nil {
		t.Fatalf("Parse rejected a blob over its reserved bytes: %v", err)
	}
	decoded, err := DecodeFloat32(parsed)
	if err != nil {
		t.Fatalf("DecodeFloat32: %v", err)
	}
	if !reflect.DeepEqual(decoded, tokens) {
		t.Fatalf("decoded %v, want %v", decoded, tokens)
	}
}

// TestScalarRounding covers the narrowing to binary16 the scalar section
// performs, which the exactly representable values used elsewhere avoid: every
// value must read back within binary16's relative error, and re-writing what
// was read back must reproduce the same bytes.
func TestScalarRounding(t *testing.T) {
	// binary16 keeps 11 significand bits, so a normal value's relative error
	// is at most 2^-11. RQ1's Step for unit-norm tokens is around 0.1; the
	// sweep covers three orders of magnitude either side.
	const maxRelErr = 1.0 / 2048

	scalars := []float32{}
	for v := float32(0.001); v < 1000; v *= 1.07 {
		scalars = append(scalars, v, -v)
	}
	// zero, and a few Step values of the size unit-norm tokens produce
	scalars = append(scalars, 0, 0.10135, 0.12435, 0.04175, 0.13333)

	blob, err := build(Header{
		Encoding: EncodingFloat32, Layout: LayoutTokenMajor, ScalarKind: ScalarFloat16,
		Dims: 1, Tokens: uint32(len(scalars)),
	}, scalars, make([]byte, len(scalars)*4))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	parsed, err := Parse(blob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	for i, want := range scalars {
		got := parsed.Scalar(i)
		if diff := math.Abs(float64(got - want)); diff > maxRelErr*math.Abs(float64(want)) {
			t.Fatalf("Scalar(%d) = %v, want %v (relative error %g > %g)",
				i, got, want, diff/math.Abs(float64(want)), maxRelErr)
		}
	}

	// the loss is taken once, at write: re-writing what was read back must
	// reproduce the same bytes, so a reindex cannot drift
	reread := make([]float32, len(scalars))
	for i := range reread {
		reread[i] = parsed.Scalar(i)
	}
	again, err := build(parsed.Header, reread, make([]byte, len(scalars)*4))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !bytes.Equal(blob, again) {
		t.Fatal("re-writing the scalars read back produced different bytes")
	}
}

// TestFixedOverhead pins the size properties that do not depend on the
// encoding: the header is a fixed 24 bytes, and a blob carries no per-token
// overhead beyond its code and its scalar.
func TestFixedOverhead(t *testing.T) {
	if headerLenV1 != 24 {
		t.Fatalf("header is %d bytes, expected a fixed 24", headerLenV1)
	}

	perToken, err := codeLen(EncodingFloat32, 128)
	if err != nil {
		t.Fatalf("codeLen: %v", err)
	}
	for _, n := range []int{0, 1, 32, 124, 1030} {
		for _, kind := range []ScalarKind{ScalarNone, ScalarFloat16} {
			scalarSize, err := kind.size()
			if err != nil {
				t.Fatalf("size: %v", err)
			}
			var scalars []float32
			if scalarSize > 0 {
				scalars = make([]float32, n)
			}
			blob, err := build(Header{
				Encoding: EncodingFloat32, Layout: LayoutTokenMajor, ScalarKind: kind,
				Dims: 128, Tokens: uint32(n),
			}, scalars, make([]byte, n*perToken))
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if want := headerLenV1 + n*(perToken+scalarSize); len(blob) != want {
				t.Fatalf("n=%d scalars=%d: blob is %d bytes, want %d", n, scalarSize, len(blob), want)
			}
		}
	}
}

// goldenBlob checks one golden fixture. A fixture of the version this build
// writes must equal what the build writes, and -update rewrites it first. A
// fixture of an older version is never rewritten or compared with the writer:
// it is only parsed, which is the read-compatibility check the fixtures exist
// for. After a version bump the older entries stay in the table with their
// expectations and the new version gets fixtures of its own.
//
// Arguments:
//   - file: the fixture's name under testdata/.
//   - blob: what this build writes for it; ignored for an older version.
//   - header: the header the fixture must parse to, including its version.
//
// It returns the parsed fixture.
func goldenBlob(t *testing.T, file string, blob []byte, header Header) Blob {
	t.Helper()
	path := filepath.Join("testdata", file)
	current := header.Version == Version
	if *update && current {
		if err := os.WriteFile(path, blob, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if current && !bytes.Equal(onDisk, blob) {
		t.Fatalf("%s differs from what this build writes; the format or the "+
			"encoding changed without a version bump", path)
	}

	parsed, err := Parse(onDisk)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Header != header {
		t.Fatalf("header = %+v, want %+v", parsed.Header, header)
	}
	return parsed
}

// TestGolden checks the committed float32 blobs under testdata/ against what
// this build writes, and parses them from disk. Compatibility with blobs
// already stored cannot be tested inside one version, so the blobs are
// committed and every later version must keep parsing them. Regenerate with:
//
//	go test ./adapters/repos/db/vector/multivector/packed/ -run TestGolden -update
//
// Regenerating is only legitimate when the format version is bumped; a
// difference at the current version is a compatibility break.
// Regenerating rewrites only the fixtures of the version this build writes:
// after a bump, the older versions' files stay as stored and are parsed only
// (goldenBlob), and the new version gets entries of its own.
func TestGolden(t *testing.T) {
	goldenTokens := [][]float32{
		{0, -0.5, 1.25, -2},
		{0, 0, 0, 0}, // an all-zero token
		{3.5, -0.125, 0.0625, 1},
	}

	plain, err := EncodeFloat32(goldenTokens, 4)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}

	// float32 writes no scalar section, so this second fixture is assembled at
	// the format level to pin the scalar section's layout too
	scalars := []float32{1, 0, 0.10546875}
	withScalars, err := build(Header{
		Encoding:         EncodingFloat32,
		Layout:           LayoutTokenMajor,
		ScalarKind:       ScalarFloat16,
		Dims:             4,
		Tokens:           3,
		QuantizerID:      0xfeed,
		QuantizerVersion: 2,
	}, scalars, float32Codes(goldenTokens))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	tests := []struct {
		file    string
		blob    []byte
		header  Header
		scalars []float32
		decodes bool
	}{
		{
			file: "v1_float32.blob",
			blob: plain,
			header: Header{
				Version: 1, Encoding: EncodingFloat32, Layout: LayoutTokenMajor,
				ScalarKind: ScalarNone, Dims: 4, Tokens: 3,
			},
			scalars: []float32{1, 1, 1},
			decodes: true,
		},
		{
			// pins the scalar-section layout; float32 gives the scalars no
			// meaning, so it parses and its scalars read back, but decoding
			// refuses it (checkFloat32Blob)
			file: "v1_float32_scalars.blob",
			blob: withScalars,
			header: Header{
				Version: 1, Encoding: EncodingFloat32, Layout: LayoutTokenMajor,
				ScalarKind: ScalarFloat16, Dims: 4, Tokens: 3,
				QuantizerID: 0xfeed, QuantizerVersion: 2,
			},
			scalars: scalars,
			decodes: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			parsed := goldenBlob(t, tt.file, tt.blob, tt.header)
			for i, want := range tt.scalars {
				if got := parsed.Scalar(i); got != want {
					t.Fatalf("Scalar(%d) = %v, want %v", i, got, want)
				}
			}
			decoded, err := DecodeFloat32(parsed)
			if !tt.decodes {
				if err == nil {
					t.Fatal("DecodeFloat32 accepted a float32 blob with a scalar section")
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeFloat32: %v", err)
			}
			assertTokensEqual(t, decoded, goldenTokens)
		})
	}
}
