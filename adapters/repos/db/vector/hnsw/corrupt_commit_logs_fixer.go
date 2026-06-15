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

package hnsw

import (
	"os"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

// CorruptCommitLogFixer helps identify potentially corrupt commit logs and
// tries to mitigate the problem
type CorruptCommitLogFixer struct{}

func NewCorruptedCommitLogFixer() *CorruptCommitLogFixer {
	return &CorruptCommitLogFixer{}
}

// Do tries to delete files that could be corrupt and removes them from the
// returned list, indicating that the index should no longer try to read them
//
// A file is considered corrupt if it has the .condensed suffix - yet there is
// a file with the same name without that suffix. This would indicate that
// trying to condense the file has somehow failed or been interrupted, as a
// successful condensing would have been succeeded by the removal of the
// original file. We thus assume the file must be corrupted, and delete it, so
// that the original will be used instead.
func (fixer *CorruptCommitLogFixer) Do(fileNames []string) ([]string, error) {
	out := make([]string, 0, len(fileNames))

	// keep processing remaining files even if a delete fails, so the returned
	// list always reflects every surviving (non-corrupt) file rather than
	// being truncated at the first error
	ec := errorcompounder.New()
	for _, fileName := range fileNames {
		if !strings.HasSuffix(fileName, ".condensed") {
			// has no suffix, so it can never be considered corrupt
			out = append(out, fileName)
			continue
		}

		// this file has a suffix, check if one without the suffix exists as well
		if !slices.Contains(fileNames, strings.TrimSuffix(fileName, ".condensed")) {
			// does not seem corrupt, proceed
			out = append(out, fileName)
			continue
		}

		// we have found a corrupt file, delete it and do not append it to the
		// list. The index must not read it even if the delete fails.
		if err := os.Remove(fileName); err != nil {
			ec.Add(errors.Wrapf(err, "delete corrupt commit log file %q", fileName))
		}
	}

	return out, ec.ToError()
}
