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

// Package tokenizer serves tiktoken's BPE vocabularies from a local directory
// instead of fetching them from OpenAI's blob store at runtime.
package tokenizer

import (
	"encoding/base64"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/weaviate/tiktoken-go"
)

// localBpeLoader serves a vocabulary from dir (named by the blob's base, e.g.
// cl100k_base.tiktoken), deferring to fallback for anything not found there.
type localBpeLoader struct {
	dir      string
	fallback tiktoken.BpeLoader
}

func (l *localBpeLoader) LoadTiktokenBpe(tiktokenBpeFile string) (map[string]int, error) {
	contents, err := os.ReadFile(filepath.Join(l.dir, path.Base(tiktokenBpeFile)))
	switch {
	case err == nil:
		return parseTiktokenBpe(contents)
	case !errors.Is(err, os.ErrNotExist):
		// present but unreadable: surface it rather than fall back to the network.
		return nil, err
	default:
		return l.fallback.LoadTiktokenBpe(tiktokenBpeFile)
	}
}

// parseTiktokenBpe decodes tiktoken's "<base64-token> <rank>" vocabulary lines.
func parseTiktokenBpe(contents []byte) (map[string]int, error) {
	ranks := make(map[string]int)
	for _, line := range strings.Split(string(contents), "\n") {
		if line == "" {
			continue
		}
		token, rank, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			return nil, err
		}
		r, err := strconv.Atoi(rank)
		if err != nil {
			return nil, err
		}
		ranks[string(decoded)] = r
	}
	return ranks, nil
}

// RegisterLocalBPE makes tiktoken read *.tiktoken files from dir before its
// network loader. No-op when dir is empty. Call once at startup, before any
// tokenization.
func RegisterLocalBPE(dir string) {
	if dir == "" {
		return
	}
	tiktoken.SetBpeLoader(&localBpeLoader{dir: dir, fallback: tiktoken.NewDefaultBpeLoader()})
}
