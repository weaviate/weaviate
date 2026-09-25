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

// Package 'packed' defines a compressed, self-describing byte format for one
// multi-vector (one document's token vectors), and MaxSim scorers that read it
// directly, without rebuilding the [][]float32 first. An approximate scoring
// stage can run over these blobs and cut how many candidates reach the exact
// rescoring stage, which reads the whole object record of each candidate and
// allocates one slice per token.
//
// # Layout
//
//	+--------+-----------------+-------------------+
//	| header | per-token codes | per-token scalars |
//	+--------+-----------------+-------------------+
//
// Codes and scalars are two separate contiguous sections, each with a fixed
// stride. The code section starts right after the header, at a fixed offset
// that is a multiple of 8, so a kernel walks the codes sequentially; the
// scalar section is one contiguous array the kernel multiplies its raw
// distances by in a single pass. The scalar section is small (2 bytes per
// token) and stays in cache next to the codes.
//
// No section offsets are stored: the code section's length follows from the
// encoding and the token count, and the scalars are whatever remains. The
// header stores its own length, and Parse checks it against the version,
// since each version writes exactly one header length.
//
// The header is 24 bytes: a 4-byte magic, so a value that is not a packed
// blob fails with probability about 2^-32; one byte each for version,
// encoding, layout and scalar kind; the header length; dims and token count;
// a quantizer reference (a uint16 id and a uint16 version) naming the trained
// parameters a scorer needs; and 4 reserved bytes, written zero and ignored on
// read, free for future use. The quantizer parameters themselves are not in
// the blob: they are persisted the way compressionhelpers persists quantizer
// state, through PersistCompression and the commit log. The sentinel errors
// below follow adapters/repos/db/queue.
//
// # Byte handling
//
// All integers are little-endian and read through binary.LittleEndian. A byte
// slice at an interior offset has no alignment guarantee, so it is never
// reinterpreted as []uint64 even where the layout would allow it. The
// binary.LittleEndian accessors compile to one unaligned load on amd64 and
// arm64. binary.Read is not used anywhere here: it allocates.
package packed

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/tphakala/simd/f16"
)

// Version is the format version this package writes. Readers accept anything
// up to and including it.
const Version uint8 = 1

// Parse returns these sentinel errors so a caller can tell the three "not the
// blob you expected" cases apart. A code read under the wrong quantizer has the
// same length as a right one and would score silently wrong, so the header has
// to make the mismatch visible. Same pattern as errBadMagic and
// errUnknownVersion in adapters/repos/db/queue.
var (
	// ErrBadMagic means the value is not a packed blob at all (a mis-keyed
	// read, or corruption).
	ErrBadMagic = errors.New("packed: bad magic")

	// ErrUnsupportedVersion means it is a packed blob written by a newer build
	// than this one can read.
	ErrUnsupportedVersion = errors.New("packed: unsupported format version")

	// ErrUnknownEncoding means it is a readable packed blob whose encoding or
	// scalar kind this build does not implement.
	ErrUnknownEncoding = errors.New("packed: unknown encoding")
)

// magic prefixes every blob, so a mis-keyed LSM value is rejected instead of
// parsed into nonsense.
const magic = "WVPM"

// headerLenV1 is the header size version 1 writes. Readers do not parse with
// it; they read the headerLen field and check it against the version. It is
// the floor for every version, since header fields are only ever appended.
const headerLenV1 = 24

// Header field offsets, in bytes. Bytes 20..23 are reserved: written zero,
// ignored on read (see the package comment).
const (
	offMagic        = 0  // 4 bytes
	offVersion      = 4  // 1
	offEncoding     = 5  // 1
	offLayout       = 6  // 1
	offScalarKind   = 7  // 1
	offHeaderLen    = 8  // 2
	offDims         = 10 // 2
	offTokens       = 12 // 4
	offQuantizerID  = 16 // 2
	offQuantizerVer = 18 // 2
	offReserved     = 20 // 4
)

// minParseLen is the shortest prefix a reader needs before it can read the
// headerLen field: the first 10 bytes, everything up to and including that
// field.
const minParseLen = offHeaderLen + 2

// Encoding identifies how one token's coordinates are packed into its code.
type Encoding uint8

// EncodingFloat32 stores coordinates as raw float32, 4*dims bytes per token.
// It is a test control: the encoding is a passthrough, so MaxSim over a
// float32 blob must equal exact float32 MaxSim, and any difference is a bug in
// the format or the scorer.
const EncodingFloat32 Encoding = 1

// EncodingRQ1 stores one sign bit per rotated coordinate: bit i of the code
// (bit 0 is the lowest bit of the first byte, bit 8 the lowest bit of the
// second byte, and so on) is set when rotated coordinate i is strictly
// positive. The rotation is compression.FastRotation under the seed in the
// quantizer parameters. The per-token scale Step is in the scalar section, and
// Step * <sign(x~), q~> estimates <x, q>. See rq1.go.
//
// The rotation rounds dims up to a multiple of 64, so the code is
// ceil(dims/64)*8 bytes per token.
const EncodingRQ1 Encoding = 2

// Layout describes the order in which token codes are stored.
type Layout uint8

const (
	// LayoutTokenMajor stores each token's code contiguously, tokens in order.
	LayoutTokenMajor Layout = 1

	// LayoutTokenBlock16 interleaves the code section in blocks of sixteen
	// tokens: within a block, byte g of all sixteen tokens' codes is stored
	// contiguously, for g ascending, and blocks follow each other in token
	// order. In-register fast scan kernels need this layout to be efficient:
	// sixteen tokens fill the byte lanes of one 128-bit vector register, and
	// wider registers (AVX2, AVX-512) take two or four blocks per pass.
	//
	// The trailing tokens%16 stay token-major, so the layout adds no padding
	// bytes; the fast scan transposes them into a scratch block once per
	// document.
	//
	// Blob.Code is meaningless under this layout. Every scorer checks the
	// layout it expects, so a blob in this layout is rejected by every scorer
	// except the fast scan's.
	LayoutTokenBlock16 Layout = 2
)

// blockTokens is how many tokens LayoutTokenBlock16 interleaves at a time:
// the byte lanes of one 128-bit vector register. Kernels over wider registers
// (AVX2, AVX-512) read several blocks per pass.
const blockTokens = 16

// interleaveBlock16 rewrites token-major codes into LayoutTokenBlock16 order
// and returns a fresh slice. The trailing tokens%blockTokens tokens are copied
// unchanged. It runs once per document at encode time, off the query path,
// and moves every byte exactly once.
//
// Arguments:
//   - codes: the token-major code section, tokens*perToken bytes.
//   - tokens: number of tokens in the document.
//   - perToken: code bytes per token.
func interleaveBlock16(codes []byte, tokens, perToken int) []byte {
	out := make([]byte, len(codes))
	blocks := tokens / blockTokens
	for b := range blocks {
		src := codes[b*blockTokens*perToken:]
		dst := out[b*blockTokens*perToken:]
		// token j's byte g moves from j*perToken+g to g*blockTokens+j
		for g := range perToken {
			for j := range blockTokens {
				dst[g*blockTokens+j] = src[j*perToken+g]
			}
		}
	}
	copy(out[blocks*blockTokens*perToken:], codes[blocks*blockTokens*perToken:])
	return out
}

// ScalarKind is the numeric type of the per-token scalar section.
type ScalarKind uint8

const (
	// ScalarNone means there is no scalar section. float32 coordinates carry
	// their own magnitude and need no rescaling.
	ScalarNone ScalarKind = 0

	// ScalarFloat16 stores one IEEE-754 binary16 per token. A binary16 scalar
	// instead of a float32 one saves 2 bytes per token, more than 10% of an
	// RQ1 token at d=128 (16 bytes of code plus the scalar), so it is worth
	// saving. binary16 keeps 11 significand bits, a relative error of at most
	// 2^-11, far below RQ1's own 1-bit quantization error.
	ScalarFloat16 ScalarKind = 1
)

// size returns the bytes one token's scalar occupies under k.
func (k ScalarKind) size() (int, error) {
	switch k {
	case ScalarNone:
		return 0, nil
	case ScalarFloat16:
		return 2, nil
	default:
		return 0, fmt.Errorf("packed: unknown scalar kind %d: %w", uint8(k), ErrUnknownEncoding)
	}
}

// codeLen returns the bytes one token's code occupies.
//
// Arguments:
//   - enc: the encoding.
//   - dims: dimensionality of each token vector.
func codeLen(enc Encoding, dims uint16) (int, error) {
	switch enc {
	case EncodingFloat32:
		return 4 * int(dims), nil
	case EncodingRQ1:
		// one sign bit per rotated coordinate; the rotation rounds dims up to
		// a multiple of 64 (compression.NewFastRotation), so whole 64-bit words
		return 8 * ((int(dims) + 63) / 64), nil
	default:
		return 0, fmt.Errorf("packed: encoding %d: %w", uint8(enc), ErrUnknownEncoding)
	}
}

// Header is the fixed-size prefix of a blob.
type Header struct {
	// Version is set by Parse. build ignores it and always writes Version.
	Version    uint8
	Encoding   Encoding
	Layout     Layout
	ScalarKind ScalarKind

	// Dims is the number of dimensions of a token vector, Tokens is the number
	// of tokens of the document. Tokens is a uint32 because the marshalled
	// object stores a multi-vector's token count as a uint32, so the encoder
	// accepts every document the object store does. Tokens may be zero: a
	// document with no tokens is legal, which is why the encoders take Dims
	// explicitly instead of reading it off the input.
	Dims   uint16
	Tokens uint32

	// QuantizerID and QuantizerVersion identify the parameter set needed by a
	// scorer; zero means the encoding has no trained parameters. The
	// parameters themselves live outside the blob: the caller hands them to
	// encoder and scorer and persists them the way compressionhelpers persists
	// quantizer state (PersistCompression, commit log).
	QuantizerID      uint16
	QuantizerVersion uint16
}

// build assembles header || codes || scalars into a new byte slice.
//
// Arguments:
//   - h: the header to write. h.Version is ignored; Version is written.
//   - scalars: one value per token when h.ScalarKind is ScalarFloat16, empty
//     when it is ScalarNone.
//   - codes: exactly h.Tokens whole token codes, already in h.Layout order.
func build(h Header, scalars []float32, codes []byte) ([]byte, error) {
	if h.Dims == 0 {
		return nil, errors.New("packed: dimensions must be non-zero")
	}
	scalarSize, err := h.ScalarKind.size()
	if err != nil {
		return nil, err
	}
	perToken, err := codeLen(h.Encoding, h.Dims)
	if err != nil {
		return nil, err
	}

	wantScalars := 0
	if h.ScalarKind != ScalarNone {
		wantScalars = int(h.Tokens)
	}
	if len(scalars) != wantScalars {
		return nil, fmt.Errorf("packed: got %d scalars, want %d", len(scalars), wantScalars)
	}
	if want := int(h.Tokens) * perToken; len(codes) != want {
		return nil, fmt.Errorf("packed: got %d code bytes, want %d", len(codes), want)
	}

	scalarBytes := int(h.Tokens) * scalarSize
	buf := make([]byte, headerLenV1+len(codes)+scalarBytes)

	copy(buf[offMagic:], magic)
	buf[offVersion] = Version
	buf[offEncoding] = uint8(h.Encoding)
	buf[offLayout] = uint8(h.Layout)
	buf[offScalarKind] = uint8(h.ScalarKind)
	binary.LittleEndian.PutUint16(buf[offHeaderLen:], headerLenV1)
	binary.LittleEndian.PutUint16(buf[offDims:], h.Dims)
	binary.LittleEndian.PutUint32(buf[offTokens:], h.Tokens)
	binary.LittleEndian.PutUint16(buf[offQuantizerID:], h.QuantizerID)
	binary.LittleEndian.PutUint16(buf[offQuantizerVer:], h.QuantizerVersion)
	// make already zeroed the reserved bytes; written explicitly because a
	// zero there is what a future field's readers rely on
	binary.LittleEndian.PutUint32(buf[offReserved:], 0)

	copy(buf[headerLenV1:], codes)

	// a kind known to size() but not written here would leave a correctly
	// sized section full of zeros, so the default fails loudly instead
	switch h.ScalarKind {
	case ScalarNone:
	case ScalarFloat16:
		scalarsAt := headerLenV1 + len(codes)
		for i, s := range scalars {
			binary.LittleEndian.PutUint16(buf[scalarsAt+2*i:], f16.FromFloat32(s))
		}
	default:
		return nil, fmt.Errorf("packed: scalar kind %d is sized but has no writer: %w",
			uint8(h.ScalarKind), ErrUnknownEncoding)
	}
	return buf, nil
}

// Blob is a parsed, read-only view over a packed multi-vector. It does not
// copy anything: both sections alias the byte slice passed to Parse.
type Blob struct {
	Header

	codes    []byte
	scalars  []byte
	perToken int
}

// Parse validates the header of b and locates its sections. It does not
// decode any token. The returned Blob aliases b.
func Parse(b []byte) (Blob, error) {
	if len(b) < minParseLen {
		return Blob{}, fmt.Errorf("packed: blob is %d bytes, too short for a header", len(b))
	}
	if string(b[offMagic:offMagic+len(magic)]) != magic {
		return Blob{}, ErrBadMagic
	}

	version := b[offVersion]
	if version == 0 || version > Version {
		return Blob{}, fmt.Errorf("packed: version %d, this build reads up to %d: %w",
			version, Version, ErrUnsupportedVersion)
	}
	// Each version writes one fixed header length, so a version-1 blob that
	// claims any other length is corrupt. Trusting the field instead would let
	// an inflated headerLen shift the code section while the totals still add
	// up, and the wrong bytes would be returned silently. A later version adds
	// its own case here.
	headerLen := int(binary.LittleEndian.Uint16(b[offHeaderLen:]))
	if headerLen != headerLenV1 {
		return Blob{}, fmt.Errorf("packed: version %d header is %d bytes, not %d",
			version, headerLen, headerLenV1)
	}
	if len(b) < headerLen {
		return Blob{}, fmt.Errorf("packed: blob is %d bytes, shorter than its %d byte header",
			len(b), headerLen)
	}

	// the reserved bytes at offReserved are not read
	h := Header{
		Version:          version,
		Encoding:         Encoding(b[offEncoding]),
		Layout:           Layout(b[offLayout]),
		ScalarKind:       ScalarKind(b[offScalarKind]),
		Dims:             binary.LittleEndian.Uint16(b[offDims:]),
		Tokens:           binary.LittleEndian.Uint32(b[offTokens:]),
		QuantizerID:      binary.LittleEndian.Uint16(b[offQuantizerID:]),
		QuantizerVersion: binary.LittleEndian.Uint16(b[offQuantizerVer:]),
	}
	if h.Dims == 0 {
		return Blob{}, errors.New("packed: header declares zero dimensions")
	}
	// Layout is not validated here. It does not affect where the sections
	// are, and each scorer rejects the layouts it cannot read.
	scalarSize, err := h.ScalarKind.size()
	if err != nil {
		return Blob{}, err
	}
	perToken, err := codeLen(h.Encoding, h.Dims)
	if err != nil {
		return Blob{}, err
	}

	codeBytes := int(h.Tokens) * perToken
	scalarBytes := int(h.Tokens) * scalarSize
	if want := headerLen + codeBytes + scalarBytes; want != len(b) {
		return Blob{}, fmt.Errorf("packed: blob is %d bytes, header describes %d", len(b), want)
	}

	return Blob{
		Header:   h,
		codes:    b[headerLen : headerLen+codeBytes],
		scalars:  b[headerLen+codeBytes:],
		perToken: perToken,
	}, nil
}

// Scalar returns token i's scalar (the binary16 Step of a ScalarFloat16
// section), or 1 when the blob has no scalar section, which is the neutral
// value under the estimator scalar * <code, q>. i must be less than Tokens.
//
// A kernel scoring a whole document should not call this per token: the
// section is contiguous so it can be widened in one pass (see widenRQ1Steps).
func (b *Blob) Scalar(i int) float32 {
	if b.ScalarKind == ScalarFloat16 {
		return f16.ToFloat32(binary.LittleEndian.Uint16(b.scalars[2*i:]))
	}
	return 1
}

// Scalars returns the raw scalar section, aliasing the blob: binary16 words
// under ScalarFloat16, empty when the blob has no scalar section.
func (b *Blob) Scalars() []byte {
	return b.scalars
}

// Code returns token i's code, aliasing the blob. i must be less than Tokens,
// and the blob must be token-major: under LayoutTokenBlock16 a token's code
// bytes are not contiguous, and this would return one byte position of sixteen
// tokens. Callers check the layout first.
func (b *Blob) Code(i int) []byte {
	return b.codes[i*b.perToken : (i+1)*b.perToken]
}

// Codes returns the whole code section, aliasing the blob.
func (b *Blob) Codes() []byte {
	return b.codes
}

// validateTokens checks that dims is in range and that every token has exactly
// dims coordinates.
//
// Arguments:
//   - tokens: one slice per token.
//   - dims: the expected dimensionality of each token vector, in [1, MaxUint16].
func validateTokens(tokens [][]float32, dims int) error {
	if dims <= 0 || dims > math.MaxUint16 {
		return fmt.Errorf("packed: dimensions %d out of range [1, %d]", dims, math.MaxUint16)
	}
	for i, t := range tokens {
		if len(t) != dims {
			return fmt.Errorf("packed: token %d has %d dimensions, want %d", i, len(t), dims)
		}
	}
	return nil
}
