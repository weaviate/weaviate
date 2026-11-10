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

/*
murmur.go, unmodified but for its package clause, is the one in
github.com/bits-and-blooms/bloom/v3 v3.7.0. The TestHashRandom it cites is in
that module's murmur_test.go. Sum and Test reproduce the module's
baseHashes and location. All of it is under the module's licence:

Copyright (c) 2014 Will Fitzgerald. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Package bloomhash tests one key against many bloom filters while hashing it
// once. The hash depends only on the key, and each filter reduces it modulo its
// own size.
package bloomhash

import "github.com/bits-and-blooms/bloom/v3"

// Hash is a key's murmur3 hash, from which bloom.BloomFilter derives its bit
// locations.
type Hash [4]uint64

func Sum(key []byte) Hash {
	var d digest128
	h1, h2, h3, h4 := d.sum256(key)
	return Hash{h1, h2, h3, h4}
}

// Test answers f.Test(key) for the key h was summed from.
func Test(f *bloom.BloomFilter, h Hash) bool {
	m := uint64(f.Cap())
	set := f.BitSet()
	for i := uint64(0); i < uint64(f.K()); i++ {
		loc := h[i%2] + i*h[2+(((i+(i%2))%4)/2)]
		if !set.Test(uint(loc % m)) {
			return false
		}
	}
	return true
}
