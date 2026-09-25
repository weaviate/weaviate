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

package db

import (
	"strconv"
	"strings"
)

// genSuffix returns the migration's generation suffix, e.g. "_2". Generation
// is the RAFT task version: monotonic, so migrations never collide on a dir
// name. Generation 0 (no suffix) is the canonical post-promotion bucket; live migrations use >= 1.
func genSuffix(generation int) string {
	return "_" + strconv.Itoa(generation)
}

// parseMigrationDirName splits a bucket directory suffix (e.g.
// "__retokenize_ingest_2") into its (prefix, generation) parts. The "prefix"
// returned is everything up to and excluding the trailing "_<N>".
//
// Returns ok=false if the input does not end with "_<positive-int>".
func parseMigrationDirName(name string) (prefix string, generation int, ok bool) {
	idx := strings.LastIndex(name, "_")
	if idx <= 0 || idx == len(name)-1 {
		return "", 0, false
	}
	gen, err := strconv.Atoi(name[idx+1:])
	if err != nil || gen < 1 {
		return "", 0, false
	}
	return name[:idx], gen, true
}
