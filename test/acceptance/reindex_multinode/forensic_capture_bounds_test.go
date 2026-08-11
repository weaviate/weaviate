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

package reindex_multinode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestAllowNextFile(t *testing.T) {
	const budget = int64(forensicCaptureByteBudget)

	tests := []struct {
		name          string
		copiedTotal   int64
		filesThisNode int
		wantStop      bool
		wantLimit     int64
		wantReason    string
	}{
		{
			name:          "a fresh capture may use the whole budget",
			copiedTotal:   0,
			filesThisNode: 0,
			wantStop:      false,
			wantLimit:     budget,
		},
		{
			name:          "a partly used budget offers only the remainder",
			copiedTotal:   budget - 100,
			filesThisNode: 1,
			wantStop:      false,
			wantLimit:     100,
		},
		{
			name:          "the last byte of budget is still offered",
			copiedTotal:   budget - 1,
			filesThisNode: 1,
			wantStop:      false,
			wantLimit:     1,
		},
		{
			name:          "an exactly exhausted budget stops the capture",
			copiedTotal:   budget,
			filesThisNode: 1,
			wantStop:      true,
			wantReason:    "byte budget",
		},
		{
			name:          "an overshot budget stops the capture",
			copiedTotal:   budget + 4096,
			filesThisNode: 1,
			wantStop:      true,
			wantReason:    "byte budget",
		},
		{
			name:          "one file below the per-node cap is still allowed",
			copiedTotal:   0,
			filesThisNode: forensicFilesPerNodeCap - 1,
			wantStop:      false,
			wantLimit:     budget,
		},
		{
			name:          "reaching the per-node cap stops the capture",
			copiedTotal:   0,
			filesThisNode: forensicFilesPerNodeCap,
			wantStop:      true,
			wantReason:    "per-node file cap",
		},
		{
			name:          "exceeding the per-node cap stops the capture",
			copiedTotal:   0,
			filesThisNode: forensicFilesPerNodeCap + 1,
			wantStop:      true,
			wantReason:    "per-node file cap",
		},
		{
			name:          "the file cap is reported when both bounds are hit",
			copiedTotal:   budget,
			filesThisNode: forensicFilesPerNodeCap,
			wantStop:      true,
			wantReason:    "per-node file cap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := allowNextFile(tt.copiedTotal, tt.filesThisNode)

			assert.Equal(t, tt.wantStop, got.stop)
			if tt.wantStop {
				assert.Contains(t, got.reason, tt.wantReason)
				return
			}
			assert.Empty(t, got.reason)
			assert.Equal(t, tt.wantLimit, got.limit)
		})
	}
}

// errReader fails after handing out its payload, standing in for a container
// stream that breaks mid-copy.
type errReader struct {
	payload []byte
	err     error
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.payload) > 0 {
		n := copy(p, r.payload)
		r.payload = r.payload[n:]
		return n, nil
	}
	return 0, r.err
}

func TestCopyBounded(t *testing.T) {
	tests := []struct {
		name          string
		src           string
		limit         int64
		wantWritten   string
		wantTruncated bool
	}{
		{
			name:          "a file smaller than the limit is copied whole",
			src:           "segment",
			limit:         64,
			wantWritten:   "segment",
			wantTruncated: false,
		},
		{
			name:          "a file exactly at the limit is not reported truncated",
			src:           "seg",
			limit:         3,
			wantWritten:   "seg",
			wantTruncated: false,
		},
		{
			name:          "a file larger than the limit is cut and reported",
			src:           "segment-and-more",
			limit:         7,
			wantWritten:   "segment",
			wantTruncated: true,
		},
		{
			name:          "one byte over the limit still counts as truncated",
			src:           "abcd",
			limit:         3,
			wantWritten:   "abc",
			wantTruncated: true,
		},
		{
			name:          "an empty file copies nothing and is not truncated",
			src:           "",
			limit:         64,
			wantWritten:   "",
			wantTruncated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dst bytes.Buffer

			written, truncated, err := copyBounded(&dst, strings.NewReader(tt.src), tt.limit)

			require.NoError(t, err)
			assert.Equal(t, tt.wantWritten, dst.String())
			assert.Equal(t, int64(len(tt.wantWritten)), written)
			assert.Equal(t, tt.wantTruncated, truncated)
		})
	}
}

func TestCopyBoundedSurfacesReadErrors(t *testing.T) {
	t.Run("a stream that breaks before the limit surfaces the error", func(t *testing.T) {
		var dst bytes.Buffer
		broken := &errReader{payload: []byte("par"), err: errors.New("stream reset")}

		written, truncated, err := copyBounded(&dst, broken, 64)

		require.Error(t, err)
		assert.EqualError(t, err, "stream reset")
		assert.False(t, truncated)
		assert.Equal(t, int64(3), written)
	})

	t.Run("a stream that breaks exactly at the limit surfaces the error", func(t *testing.T) {
		var dst bytes.Buffer
		broken := &errReader{payload: []byte("par"), err: errors.New("stream reset")}

		written, truncated, err := copyBounded(&dst, broken, 3)

		require.Error(t, err)
		assert.EqualError(t, err, "stream reset")
		assert.False(t, truncated)
		assert.Equal(t, int64(3), written)
		assert.Equal(t, "par", dst.String())
	})

	t.Run("io.EOF at the limit means the file simply ended", func(t *testing.T) {
		var dst bytes.Buffer
		exact := &errReader{payload: []byte("par"), err: io.EOF}

		written, truncated, err := copyBounded(&dst, exact, 3)

		require.NoError(t, err)
		assert.False(t, truncated)
		assert.Equal(t, int64(3), written)
	})
}

// fakeContainer stands in for a weaviate container. Only the two methods the
// capture calls are implemented; the embedded nil interface panics loudly if
// the capture ever reaches for anything else.
type fakeContainer struct {
	testcontainers.Container
	opened []string
	open   func(path string) (io.ReadCloser, error)
	exec   func(cmd []string) (int, io.Reader, error)
}

func (f *fakeContainer) CopyFileFromContainer(_ context.Context, path string) (io.ReadCloser, error) {
	f.opened = append(f.opened, path)
	return f.open(path)
}

func (f *fakeContainer) Exec(_ context.Context, cmd []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	return f.exec(cmd)
}

// newTestCapture builds a capture writing into a fresh temp dir.
func newTestCapture(t *testing.T) *forensicCapture {
	t.Helper()
	return &forensicCapture{
		log:         &captureLog{t: t, prefix: "range-count forensic capture [probe]"},
		root:        t.TempDir(),
		dataDir:     "/data",
		classDir:    "things",
		bucketMatch: "property_dateInt_rangeable",
	}
}

// artifactFiles lists every file under root, by its path inside root, so a test
// can assert on exactly what an investigator would see after unzipping.
func artifactFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	found := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		found[filepath.ToSlash(rel)] = string(body)
		return nil
	}))
	return found
}

func readerFor(s string) func(string) (io.ReadCloser, error) {
	return func(string) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(s)), nil }
}

func TestCopyFilesMirrorsContainerPaths(t *testing.T) {
	c := newTestCapture(t)
	container := &fakeContainer{open: readerFor("segment-bytes")}

	c.copyFiles(context.Background(), container, 2,
		"/data/things/shardA/lsm/property_dateInt_rangeable/segment.db\n"+
			"/data/things/shardA/lsm/property_dateInt_rangeable/segment.wal\n")

	assert.Equal(t, map[string]string{
		"node2/things/shardA/lsm/property_dateInt_rangeable/segment.db":  "segment-bytes",
		"node2/things/shardA/lsm/property_dateInt_rangeable/segment.wal": "segment-bytes",
	}, artifactFiles(t, c.root))
	assert.Equal(t, int64(2*len("segment-bytes")), c.copied)
}

func TestCopyFilesCountsBytesLeftOnDiskByABrokenCopy(t *testing.T) {
	c := newTestCapture(t)
	// Three streams that hand out 20 bytes each and then break, which is what
	// a container stream that resets mid-file looks like.
	container := &fakeContainer{open: func(string) (io.ReadCloser, error) {
		return io.NopCloser(&errReader{
			payload: []byte(strings.Repeat("x", 20)),
			err:     errors.New("stream reset"),
		}), nil
	}}

	c.copyFiles(context.Background(), container, 1,
		"/data/things/s/lsm/b/a.db\n/data/things/s/lsm/b/b.db\n/data/things/s/lsm/b/c.db")

	var onDisk int
	for _, body := range artifactFiles(t, c.root) {
		onDisk += len(body)
	}
	assert.Equal(t, 60, onDisk, "the broken copies left 60 bytes on disk")
	assert.Equal(t, int64(60), c.copied, "bytes on disk must be charged to the byte budget")
}

func TestCopyFilesMarksFilesThatAreNotFaithfulCopies(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		readErr   error
		limit     int64
		wantName  string
		wantBody  string
		wantNotes []string
	}{
		{
			name:     "a complete copy keeps its name",
			body:     "whole-segment",
			limit:    forensicCaptureByteBudget,
			wantName: "node1/things/s/lsm/b/seg.db",
			wantBody: "whole-segment",
		},
		{
			name:      "a copy cut by the budget is marked TRUNCATED",
			body:      "whole-segment",
			limit:     5,
			wantName:  "node1/things/s/lsm/b/seg.db" + truncatedSuffix,
			wantBody:  "whole",
			wantNotes: []string{"truncated /data/things/s/lsm/b/seg.db at 5 bytes"},
		},
		{
			name:      "a copy broken mid-stream is marked PARTIAL",
			body:      "whole",
			readErr:   errors.New("stream reset"),
			limit:     forensicCaptureByteBudget,
			wantName:  "node1/things/s/lsm/b/seg.db" + partialSuffix,
			wantBody:  "whole",
			wantNotes: []string{"broke after 5 bytes", "stream reset"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCapture(t)
			container := &fakeContainer{open: func(string) (io.ReadCloser, error) {
				if tt.readErr != nil {
					return io.NopCloser(&errReader{payload: []byte(tt.body), err: tt.readErr}), nil
				}
				return io.NopCloser(strings.NewReader(tt.body)), nil
			}}
			c.copied = forensicCaptureByteBudget - tt.limit

			c.copyFiles(context.Background(), container, 1, "/data/things/s/lsm/b/seg.db")

			assert.Equal(t, map[string]string{tt.wantName: tt.wantBody}, artifactFiles(t, c.root))
			for _, note := range tt.wantNotes {
				assert.Contains(t, strings.Join(c.log.lines, "\n"), note)
			}
		})
	}
}

// The rename is the one place the marking can fail open: the copy is already
// short, and a file that keeps its .db name is byte-indistinguishable from a
// whole segment. The manifest is then the only thing standing between an
// investigator and reading a cut capture as product corruption.
func TestCopyFilesKeepsTheOriginalNameWhenTheMarkerCannotBeApplied(t *testing.T) {
	c := newTestCapture(t)
	// A non-empty directory sitting on the .PARTIAL name: os.Rename cannot
	// replace it, on any unix.
	blocked := filepath.Join(c.root, "node1", "things", "s", "lsm", "b", "seg.db"+partialSuffix)
	require.NoError(t, os.MkdirAll(blocked, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(blocked, "occupied"), []byte("x"), 0o644))

	container := &fakeContainer{open: func(string) (io.ReadCloser, error) {
		return io.NopCloser(&errReader{payload: []byte("cut"), err: errors.New("stream reset")}), nil
	}}

	c.copyFiles(context.Background(), container, 1, "/data/things/s/lsm/b/seg.db")
	c.log.writeTo(c.root)

	files := artifactFiles(t, c.root)
	assert.Equal(t, "cut", files["node1/things/s/lsm/b/seg.db"],
		"the incomplete copy stays in the artifact under its original name")

	manifest := files[captureManifestName]
	assert.Contains(t, manifest, "could not mark node1/things/s/lsm/b/seg.db as "+partialSuffix,
		"the manifest is the only remaining signal that this file is not whole")
	assert.Contains(t, manifest, "it stays in the artifact under its original name")
	// The copy line must name the file as it actually is on disk, not as the
	// rename meant to leave it.
	assert.Contains(t, manifest, "kept as node1/things/s/lsm/b/seg.db:")
	assert.NotContains(t, manifest, "kept as node1/things/s/lsm/b/seg.db"+partialSuffix)
}

func TestCopyFilesRecordsWhatEachNodeContributed(t *testing.T) {
	tests := []struct {
		name        string
		alreadyUsed int64
		body        string
		paths       string
		want        string
	}{
		{
			name:  "a node that listed nothing says so",
			paths: "",
			want:  "node 1: listed 0 files, attempted 0, copied 0 whole, 0 bytes",
		},
		{
			name:  "a node that copied cleanly says how much",
			body:  "segment-bytes",
			paths: "/data/things/s/lsm/b/a.db\n/data/things/s/lsm/b/b.db",
			want:  "node 1: listed 2 files, attempted 2, copied 2 whole, 26 bytes",
		},
		{
			name:        "a truncated file is not counted as copied whole",
			alreadyUsed: forensicCaptureByteBudget - 5,
			body:        "whole-segment",
			paths:       "/data/things/s/lsm/b/a.db",
			want:        "node 1: listed 1 files, attempted 1, copied 0 whole, 5 bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCapture(t)
			c.copied = tt.alreadyUsed
			container := &fakeContainer{open: readerFor(tt.body)}

			c.copyFiles(context.Background(), container, 1, tt.paths)

			assert.Contains(t, strings.Join(c.log.lines, "\n"), tt.want)
		})
	}
}

func TestCaptureManifestNamesEveryIncompleteFile(t *testing.T) {
	c := newTestCapture(t)
	container := &fakeContainer{open: func(path string) (io.ReadCloser, error) {
		if strings.HasSuffix(path, "broken.db") {
			return io.NopCloser(&errReader{payload: []byte("ab"), err: errors.New("stream reset")}), nil
		}
		return io.NopCloser(strings.NewReader("ok")), nil
	}}

	c.copyFiles(context.Background(), container, 1, "/data/things/s/lsm/b/good.db\n/data/things/s/lsm/b/broken.db")
	c.log.writeTo(c.root)

	manifest := artifactFiles(t, c.root)[captureManifestName]
	require.NotEmpty(t, manifest, "the artifact must carry its own record of what the capture did")
	assert.Contains(t, manifest, "broken.db"+partialSuffix)
	assert.Contains(t, manifest, "stream reset")
	assert.NotContains(t, manifest, "good.db"+partialSuffix)
}

func TestCopyFilesStopsWhenCaptureWindowClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := newTestCapture(t)
	container := &fakeContainer{open: readerFor("never read")}

	c.copyFiles(ctx, container, 1, "/data/things/s/lsm/b/a.db\n/data/things/s/lsm/b/b.db\n/data/things/s/lsm/b/c.db")

	assert.Empty(t, container.opened, "a closed window must stop the loop before the first copy")
	assert.Zero(t, c.copied)
	assert.Contains(t, strings.Join(c.log.lines, "\n"), "3 of 3 files not attempted")
}

func TestCopyFilesStopsWhenTheByteBudgetIsSpent(t *testing.T) {
	c := newTestCapture(t)
	c.copied = forensicCaptureByteBudget
	container := &fakeContainer{open: readerFor("never read")}

	c.copyFiles(context.Background(), container, 1, "/data/things/s/lsm/b/a.db\n/data/things/s/lsm/b/b.db")

	assert.Empty(t, container.opened)
	assert.Contains(t, strings.Join(c.log.lines, "\n"), "2 of 2 files not attempted")
}

func TestCopyFilesRecordsAFileItCouldNotOpen(t *testing.T) {
	c := newTestCapture(t)
	container := &fakeContainer{open: func(string) (io.ReadCloser, error) {
		return nil, errors.New("no such file or directory")
	}}

	c.copyFiles(context.Background(), container, 1, "/data/things/s/lsm/b/gone.db")

	assert.Empty(t, artifactFiles(t, c.root))
	assert.Zero(t, c.copied)
	assert.Contains(t, strings.Join(c.log.lines, "\n"),
		"copy /data/things/s/lsm/b/gone.db failed, no file written: no such file or directory")
}

// A destination the capture cannot write is a path that appears in no artifact
// file, so the record has to be what names it.
func TestCopyFilesRecordsAFileItCouldNotWrite(t *testing.T) {
	tests := []struct {
		name    string
		blocker func(t *testing.T, root string)
	}{
		{
			name: "a file where the destination directory has to go",
			blocker: func(t *testing.T, root string) {
				require.NoError(t, os.MkdirAll(filepath.Join(root, "node1", "things", "s", "lsm"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(root, "node1", "things", "s", "lsm", "b"),
					[]byte("x"), 0o644))
			},
		},
		{
			name: "a directory where the destination file has to go",
			blocker: func(t *testing.T, root string) {
				require.NoError(t, os.MkdirAll(
					filepath.Join(root, "node1", "things", "s", "lsm", "b", "seg.db"), 0o755))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCapture(t)
			tt.blocker(t, c.root)
			container := &fakeContainer{open: readerFor("segment-bytes")}

			c.copyFiles(context.Background(), container, 1, "/data/things/s/lsm/b/seg.db")

			assert.Zero(t, c.copied, "nothing reached the disk, so nothing may be charged")
			assert.Contains(t, strings.Join(c.log.lines, "\n"),
				"node 1 copy /data/things/s/lsm/b/seg.db failed, no file written:")
		})
	}
}

// fakeNode answers the two commands collectNode runs, dispatching on which one
// it was handed. Only the manifest script sets a shell variable first.
func fakeNode(manifest string, manifestCode int, fileList string, fileListCode int) func([]string) (int, io.Reader, error) {
	return func(cmd []string) (int, io.Reader, error) {
		if script := cmd[len(cmd)-1]; strings.HasPrefix(script, "d=") {
			return manifestCode, strings.NewReader(manifest), nil
		}
		return fileListCode, strings.NewReader(fileList), nil
	}
}

func TestCollectNodeRecordsTheManifestAndCopiesTheFilesItNames(t *testing.T) {
	c := newTestCapture(t)
	container := &fakeContainer{
		exec: fakeNode("### /data/things/shardA/lsm\n", 0, "/data/things/shardA/lsm/b/seg.db\n", 0),
		open: readerFor("segment-bytes"),
	}

	c.collectNode(context.Background(), container, 3)

	assert.Contains(t, strings.Join(c.log.lines, "\n"), "node 3 rangeable bucket manifest:\n### /data/things/shardA/lsm")
	assert.Equal(t, map[string]string{"node3/things/shardA/lsm/b/seg.db": "segment-bytes"},
		artifactFiles(t, c.root))
}

func TestCollectNodeSaysSoWhenTheCollectorItselfBroke(t *testing.T) {
	c := newTestCapture(t)
	container := &fakeContainer{
		exec: fakeNode("partial output", 2, "", 1),
		open: readerFor("never read"),
	}

	c.collectNode(context.Background(), container, 1)

	record := strings.Join(c.log.lines, "\n")
	// A broken collector and a node with genuinely no rangeable data must not
	// leave the same record behind.
	assert.Contains(t, record, "node 1 manifest command failed (exit code 2)")
	assert.Contains(t, record, "partial output")
	assert.Contains(t, record, "node 1 file list command failed (exit code 1)")
	assert.Empty(t, container.opened)
}

func TestCollectNodeReportsTheCapEvenWhenTheCommandAlsoFailed(t *testing.T) {
	// One line short of the cap, then enough to run past it.
	longManifest := strings.Repeat("### /data/things/s/lsm\n", forensicExecOutputCap/23+10)

	tests := []struct {
		name     string
		manifest string
		code     int
		fileList string
		wantHas  []string
		wantMiss []string
	}{
		{
			name:     "a command that failed and was cut reports both",
			manifest: longManifest,
			code:     2,
			wantHas: []string{
				"node 1 manifest command failed (exit code 2)",
				"node 1 manifest output was cut at 1048576 bytes",
			},
		},
		{
			name:     "a cut that kept no whole line says nothing of it is shown",
			manifest: strings.Repeat("a", forensicExecOutputCap+1),
			code:     0,
			wantHas: []string{
				"node 1 manifest output was cut at 1048576 bytes; no whole line survived the cut",
			},
		},
		{
			name:     "a cut file list says how much of it reached the copy",
			manifest: "### /data/things/s/lsm\n",
			code:     0,
			fileList: strings.Repeat("a", forensicExecOutputCap+1),
			wantHas: []string{
				"node 1 file list was cut at 1048576 bytes",
				"the files past the cut were never offered to the copy; no whole line survived the cut",
				"node 1: listed 0 files",
			},
		},
		{
			name:     "output inside the cap is not reported as cut",
			manifest: "### /data/things/s/lsm\n",
			code:     0,
			wantMiss: []string{"was cut at"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCapture(t)
			container := &fakeContainer{
				exec: fakeNode(tt.manifest, tt.code, tt.fileList, 0),
				open: readerFor("never read"),
			}

			c.collectNode(context.Background(), container, 1)

			record := strings.Join(c.log.lines, "\n")
			for _, want := range tt.wantHas {
				assert.Contains(t, record, want)
			}
			for _, miss := range tt.wantMiss {
				assert.NotContains(t, record, miss)
			}
		})
	}
}

// Every other way a captured file can be wrong is marked on the file. A copy
// taken from running nodes is not, so the record has to say it.
func TestCollectAllSaysTheCopyWasTakenFromRunningNodes(t *testing.T) {
	c := newTestCapture(t)
	node := &fakeContainer{
		exec: fakeNode("### /data/things/shardA/lsm\n", 0, "", 0),
		open: readerFor("never read"),
	}

	c.collectAll(context.Background(), []testcontainers.Container{node, node})
	c.log.writeTo(c.root)

	manifest := artifactFiles(t, c.root)[captureManifestName]
	assert.Contains(t, manifest, "not a point-in-time snapshot")
	assert.Contains(t, manifest, "a .wal can have a torn tail")
	assert.Contains(t, manifest, "keep their original names")
	assert.Contains(t, manifest, "polled for "+rangeCountConvergenceWindow.String()+" before this ran")
	assert.Contains(t, manifest, "artifact root = "+c.root)
	assert.Contains(t, manifest, "copied ~0 bytes from 2 of 2 nodes")
}

func TestCollectAllTellsAnEmptyClusterApartFromAClosedWindow(t *testing.T) {
	tests := []struct {
		name     string
		nodes    int
		closed   bool
		wantHas  []string
		wantMiss []string
	}{
		{
			name:    "a compose with no weaviate nodes says so",
			nodes:   0,
			wantHas: []string{"no weaviate containers found", "copied ~0 bytes from 0 of 0 nodes"},
		},
		{
			name:     "a window that closed first is not reported as an empty cluster",
			nodes:    3,
			closed:   true,
			wantHas:  []string{"capture window closed", "after 0 of 3 nodes"},
			wantMiss: []string{"no weaviate containers found"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.closed {
				cancel()
			}
			c := newTestCapture(t)
			var nodes []testcontainers.Container
			for i := 0; i < tt.nodes; i++ {
				nodes = append(nodes, &fakeContainer{
					exec: fakeNode("", 0, "", 0),
					open: readerFor("never read"),
				})
			}

			c.collectAll(ctx, nodes)

			record := strings.Join(c.log.lines, "\n")
			for _, want := range tt.wantHas {
				assert.Contains(t, record, want)
			}
			for _, miss := range tt.wantMiss {
				assert.NotContains(t, record, miss)
			}
		})
	}
}

// A capture that cannot create its artifact dir must stop before it touches a
// container, or it logs one failure per file for every node.
func TestCaptureStopsBeforeItTouchesAnyContainer(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o644))
	t.Setenv("REINDEX_FORENSICS_DIR", blocked)

	// A nil compose panics on the first node lookup, so returning at all is the
	// proof that no lookup happened.
	assert.NotPanics(t, func() {
		captureRangeableDataDirsOnFailure(t, nil, "Things", "dateInt", "pre-restart")
	})
}

func execReturning(code int, out string, err error) func([]string) (int, io.Reader, error) {
	return func([]string) (int, io.Reader, error) { return code, strings.NewReader(out), err }
}

func TestExecCollectSurfacesFailures(t *testing.T) {
	tests := []struct {
		name       string
		exec       func([]string) (int, io.Reader, error)
		wantOut    string
		wantCut    bool
		wantErr    string
		wantNoErr  bool
		wantOutHas string
	}{
		{
			name:      "a command that succeeds returns its output and no error",
			exec:      execReturning(0, "### /data/things/s/lsm\n", nil),
			wantOut:   "### /data/things/s/lsm\n",
			wantNoErr: true,
		},
		{
			name:    "a non-zero exit code is surfaced, not swallowed",
			exec:    execReturning(2, "find: permission denied\n", nil),
			wantOut: "find: permission denied\n",
			wantErr: "exit code 2",
		},
		{
			name: "an unreadable stream is surfaced, not reported as no output",
			exec: func([]string) (int, io.Reader, error) {
				return 0, &errReader{payload: []byte("half"), err: errors.New("stdcopy broke")}, nil
			},
			wantOut: "half",
			wantErr: "stdcopy broke",
		},
		{
			name:    "a container that refuses the exec is surfaced",
			exec:    execReturning(0, "", errors.New("container exec attach: no such container")),
			wantErr: "no such container",
		},
		{
			name: "output past the cap is cut at a line boundary and reported",
			exec: execReturning(0,
				strings.Repeat("/data/things/s/lsm/b/x.db\n", forensicExecOutputCap/26+10), nil),
			wantCut:    true,
			wantErr:    "",
			wantNoErr:  true,
			wantOutHas: "/data/things/s/lsm/b/x.db\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, cut, err := execCollect(context.Background(), &fakeContainer{exec: tt.exec}, []string{"sh", "-c", "true"})

			if tt.wantNoErr {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
			assert.Equal(t, tt.wantCut, cut)
			if tt.wantOutHas != "" {
				assert.True(t, strings.HasSuffix(out, tt.wantOutHas), "a cut list must end on a whole path")
				assert.LessOrEqual(t, len(out), forensicExecOutputCap)
			} else {
				assert.Equal(t, tt.wantOut, out)
			}
		})
	}
}

func TestExecCollectReturnsWhenTheContainerNeverDoes(t *testing.T) {
	// A wedged container blocks inside Exec with nothing closing the connection
	// the context was supposed to bound.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	wedged := &fakeContainer{exec: func([]string) (int, io.Reader, error) {
		<-release
		return 0, strings.NewReader(""), nil
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()

	out, cut, err := execCollect(ctx, wedged, []string{"sh", "-c", "sleep forever"})

	assert.Less(t, time.Since(start), 5*time.Second, "capture must not wait on a wedged container")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not return inside the capture window")
	assert.Empty(t, out)
	assert.False(t, cut)
}

func TestCapExecOutput(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		want     string
		wantCut  bool
		wantSize int
	}{
		{
			name: "output below the cap is untouched",
			in:   "a\nb\n",
			want: "a\nb\n",
		},
		{
			name: "output exactly at the cap is untouched",
			in:   strings.Repeat("a", forensicExecOutputCap),
			want: strings.Repeat("a", forensicExecOutputCap),
		},
		{
			name:     "output past the cap is cut back to the last whole line",
			in:       strings.Repeat("ab\n", forensicExecOutputCap),
			wantCut:  true,
			wantSize: forensicExecOutputCap / 3 * 3,
		},
		{
			name:    "output past the cap with no line break keeps nothing",
			in:      strings.Repeat("a", forensicExecOutputCap+1),
			want:    "",
			wantCut: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, cut := capExecOutput(tt.in)

			assert.Equal(t, tt.wantCut, cut)
			if tt.wantSize > 0 {
				assert.Len(t, got, tt.wantSize)
				assert.True(t, strings.HasSuffix(got, "\n"))
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// runManifestScript runs the real script against a real shell, so what is
// asserted is the script's behavior rather than its text.
func runManifestScript(t *testing.T, dataDir string) string {
	t.Helper()
	c := &forensicCapture{dataDir: dataDir, classDir: "things", bucketMatch: "property_dateInt_rangeable"}
	out, err := exec.Command("sh", "-c", c.manifestScript()).CombinedOutput()
	require.NoError(t, err, "manifest script must exit cleanly: %s", out)
	return string(out)
}

func TestManifestScriptExplainsEveryEmptyCase(t *testing.T) {
	tests := []struct {
		name     string
		dirs     []string
		files    []string
		wantHas  []string
		wantMiss []string
	}{
		{
			name:    "a missing class dir says so and lists what is there instead",
			dirs:    []string{"somethingelse"},
			wantHas: []string{"no ", "/things dir", "somethingelse"},
		},
		{
			name:    "a class dir with no lsm dirs says so and lists what is there",
			dirs:    []string{"things/shardA"},
			wantHas: []string{"no */lsm dirs under", "shardA"},
		},
		{
			name:    "an lsm dir with no matching bucket falls back to a full listing",
			dirs:    []string{"things/shardA/lsm/property_other_rangeable"},
			wantHas: []string{"### ", "no property_dateInt_rangeable* bucket dirs found", "property_other_rangeable"},
		},
		{
			name:     "a matching bucket is listed",
			dirs:     []string{"things/shardA/lsm/property_dateInt_rangeable__rangeable_ingest_1"},
			files:    []string{"things/shardA/lsm/property_dateInt_rangeable__rangeable_ingest_1/segment.db"},
			wantHas:  []string{"### ", "property_dateInt_rangeable__rangeable_ingest_1", "segment.db"},
			wantMiss: []string{"no property_dateInt_rangeable* bucket dirs found"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()
			for _, d := range tt.dirs {
				require.NoError(t, os.MkdirAll(filepath.Join(dataDir, d), 0o755))
			}
			for _, f := range tt.files {
				require.NoError(t, os.WriteFile(filepath.Join(dataDir, f), []byte("x"), 0o644))
			}

			out := runManifestScript(t, dataDir)

			assert.NotEmpty(t, strings.TrimSpace(out), "an empty manifest cannot be told apart from no data")
			for _, want := range tt.wantHas {
				assert.Contains(t, out, want)
			}
			for _, miss := range tt.wantMiss {
				assert.NotContains(t, out, miss)
			}
		})
	}
}

// A search that could not read a directory and a search that found nothing are
// the same empty result. Only the exit status tells them apart, and the script
// prints its "nothing found" sentence off that result.
func TestManifestScriptSaysWhenTheSearchItselfFailed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads unreadable directories, so find cannot be made to fail this way")
	}
	dataDir := t.TempDir()
	unreadable := filepath.Join(dataDir, "things", "shardA", "lsm", "blocked")
	require.NoError(t, os.MkdirAll(unreadable, 0o755))
	require.NoError(t, os.Chmod(unreadable, 0o000))
	// t.TempDir cleanup cannot descend into a 000 dir.
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o755) })

	out := runManifestScript(t, dataDir)

	assert.Contains(t, out, "exited 1; what it listed may be incomplete")
	assert.NotContains(t, out, "no property_dateInt_rangeable* bucket dirs found",
		"a search that broke must not be reported as a search that found nothing")
	assert.Contains(t, out, "blocked", "the listing that follows must still be there")
}

func TestForensicArtifactRootHonorsTheCIEnvAndIsUniquePerCapture(t *testing.T) {
	base := t.TempDir()
	t.Setenv("REINDEX_FORENSICS_DIR", base)

	first, err := forensicArtifactRoot(t, "Things", "pre-restart")
	require.NoError(t, err)
	second, err := forensicArtifactRoot(t, "Things", "post-restart")
	require.NoError(t, err)

	// The workflow uploads REINDEX_FORENSICS_DIR, so a root outside it is a
	// capture CI would silently drop.
	for _, root := range []string{first, second} {
		assert.True(t, strings.HasPrefix(root, base), "%s must be under %s", root, base)
		assert.DirExists(t, root)
	}
	assert.NotEqual(t, first, second, "two captures in one run must not overwrite each other")
}

// Off CI nothing sets the env var, and a capture that landed nowhere would be
// a local run with no way to see what it found.
func TestForensicArtifactRootFallsBackToATempDir(t *testing.T) {
	t.Setenv("REINDEX_FORENSICS_DIR", "")
	fallback := filepath.Join(os.TempDir(), "reindex-forensics")

	root, err := forensicArtifactRoot(t, "Things", "pre-restart")

	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(root)
		// Only succeeds while no other capture is using it.
		_ = os.Remove(fallback)
	})
	assert.True(t, strings.HasPrefix(root, fallback+string(filepath.Separator)),
		"%s must be under %s", root, fallback)
	assert.DirExists(t, root)
}

func TestForensicArtifactRootReportsAnUnusableBase(t *testing.T) {
	// A regular file where the base dir should be: MkdirAll cannot succeed, and
	// a capture that carried on would log one failure per file instead.
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o644))
	t.Setenv("REINDEX_FORENSICS_DIR", blocked)

	root, err := forensicArtifactRoot(t, "Things", "pre-restart")

	require.Error(t, err)
	assert.Empty(t, root)
}

func TestIncompleteSuffix(t *testing.T) {
	tests := []struct {
		name      string
		truncated bool
		err       error
		want      string
	}{
		{name: "a faithful copy is not marked", want: ""},
		{name: "a truncated copy is marked", truncated: true, want: truncatedSuffix},
		{name: "a broken copy is marked", err: errors.New("boom"), want: partialSuffix},
		{
			name:      "a copy that was both broken and cut reports the break",
			truncated: true,
			err:       errors.New("boom"),
			want:      partialSuffix,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, incompleteSuffix(tt.truncated, tt.err))
		})
	}
}

func TestSplitPaths(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "no output yields no paths", in: "", want: nil},
		{name: "only whitespace yields no paths", in: "\n  \n\n", want: nil},
		{
			name: "blank and padded lines are dropped",
			in:   "  /data/a.db  \n\n/data/b.db\n",
			want: []string{"/data/a.db", "/data/b.db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, splitPaths(tt.in))
		})
	}
}

// captureLog writes into the artifact, so its own failure to write must not be
// what stops a capture.
func TestCaptureLogSurvivesAnUnwritableRoot(t *testing.T) {
	c := &captureLog{t: t, prefix: "probe"}
	c.recordf("something happened")

	assert.NotPanics(t, func() { c.writeTo(filepath.Join(t.TempDir(), "missing", "deeper")) })
	assert.Equal(t, []string{"something happened"}, c.lines)
}

// A manifest that the write cut short keeps the bytes it managed to write, so
// without a terminator it reads exactly like a complete one.
func TestCaptureManifestEndsWithATerminator(t *testing.T) {
	c := &captureLog{t: t, prefix: "probe"}
	c.recordf("first")
	c.recordf("second")
	root := t.TempDir()

	c.writeTo(root)

	body := artifactFiles(t, root)[captureManifestName]
	require.NotEmpty(t, body)
	assert.True(t, strings.HasSuffix(body, "=== END OF CAPTURE MANIFEST, 2 records ===\n"),
		"a cut manifest must be tellable from a complete one, got:\n%s", body)
}
