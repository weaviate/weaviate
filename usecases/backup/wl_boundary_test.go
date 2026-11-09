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

package backup

import (
	"errors"
	"go/build"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	moduleRoot = "github.com/weaviate/weaviate"
	wlRoot     = moduleRoot + "/wl"
)

// TestNoWLImports pins that backup, restore and the participant side run without wl/ code on an unlicensed node.
func TestNoWLImports(t *testing.T) {
	repo, err := filepath.Abs("../..")
	require.NoError(t, err)
	roots := []string{
		moduleRoot + "/usecases/backup",
		moduleRoot + "/adapters/repos/db",
		moduleRoot + "/adapters/handlers/rest/clusterapi",
	}
	for _, root := range roots {
		t.Run(root, func(t *testing.T) {
			chains, visited, err := wlImportChains(repo, root)
			require.NoError(t, err)
			require.Greater(t, visited, 1, "the walk must reach in-module imports")
			require.Empty(t, chains)
		})
	}
}

// wlImportChains walks root's test and transitive production imports inside the module and returns every chain ending under wl/.
func wlImportChains(repo, root string) ([]string, int, error) {
	parent := map[string]string{root: ""}
	queue := []string{root}
	var chains []string
	for len(queue) > 0 {
		pkg := queue[0]
		queue = queue[1:]
		if pkg == wlRoot || strings.HasPrefix(pkg, wlRoot+"/") {
			chain := []string{pkg}
			for p := parent[pkg]; p != ""; p = parent[p] {
				chain = append(chain, p)
			}
			slices.Reverse(chain)
			chains = append(chains, strings.Join(chain, " -> "))
			continue
		}
		dir := filepath.Join(repo, filepath.FromSlash(strings.TrimPrefix(pkg, moduleRoot)))
		p, err := build.Default.ImportDir(dir, 0)
		if err != nil {
			var noGo *build.NoGoError
			if errors.As(err, &noGo) {
				continue
			}
			return nil, 0, err
		}
		imports := p.Imports
		if pkg == root {
			imports = slices.Concat(p.Imports, p.TestImports, p.XTestImports)
		}
		for _, imp := range imports {
			if imp != moduleRoot && !strings.HasPrefix(imp, moduleRoot+"/") {
				continue
			}
			if _, seen := parent[imp]; seen {
				continue
			}
			parent[imp] = pkg
			queue = append(queue, imp)
		}
	}
	return chains, len(parent), nil
}
