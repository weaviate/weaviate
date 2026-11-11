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

//go:build postingedgebench

// The partition and the assignment of a build, kept on disk between runs of
// the benchmark. Neither depends on the codes, so the cells of a code sweep
// and the edge recall grid share them; at ten million vectors computing them
// takes hours, most of a build.

package postingedge

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

var benchBuildCache = flag.String("bench.buildcache", "", "postingedge: directory to keep the partition and the assignment of a build in. "+
	"A later build of the same train set, posting size and build probe takes them from there instead of computing them")

// buildCacheMeta describes a cached partition and assignment: the shape of
// the build they were computed for and the train set they were computed over.
type buildCacheMeta struct {
	Vectors   int    `json:"vectors"`
	Dims      int    `json:"dims"`
	Postings  int    `json:"postings"`
	Probe     int    `json:"probe"`
	Width     int    `json:"width"`
	Centroids int    `json:"centroids"`
	Train     string `json:"train"`
}

// buildCacheDir is where the partition and the assignment of a build of the
// dataset are kept. The seed of the partition is the one every build of the
// benchmark uses.
func buildCacheDir(data *benchData, posting, probe int) string {
	return filepath.Join(*benchBuildCache,
		fmt.Sprintf("%s-%dx%d-posting%d-probe%d-seed1", data.name, data.rows, data.dims, posting, probe))
}

// trainFingerprint tells one train file from another without reading it all:
// its size and a hash of its first mebibyte.
func trainFingerprint(dir string) (string, error) {
	file, err := os.Open(filepath.Join(dir, "train.f32"))
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	hash := fnv.New64a()
	if _, err := io.CopyN(hash, file, 1<<20); err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return fmt.Sprintf("%d bytes, fnv64a of the first MiB %016x", info.Size(), hash.Sum64()), nil
}

// saveBuildShare writes a filled share to dir, the meta file last: a
// directory without it holds nothing a build may take.
func saveBuildShare(dir, train string, share *buildShare) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	dims := len(share.centroids[0])
	err := writeCacheFile(filepath.Join(dir, "centroids.f32"), func(w *bufio.Writer) error {
		bits := make([]uint32, dims)
		for _, centroid := range share.centroids {
			for i, x := range centroid {
				bits[i] = math.Float32bits(x)
			}
			if err := writeUint32s(w, bits); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for name, values := range map[string][]uint32{"primary.u32": share.assigned.primary, "probes.u32": share.assigned.probes} {
		err := writeCacheFile(filepath.Join(dir, name), func(w *bufio.Writer) error { return writeUint32s(w, values) })
		if err != nil {
			return err
		}
	}
	meta, err := json.MarshalIndent(buildCacheMeta{
		Vectors: share.vectors, Dims: dims, Postings: share.postings, Probe: share.probe,
		Width: share.assigned.width, Centroids: len(share.centroids), Train: train,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "meta.json"), meta, 0o644)
}

// loadBuildShare reads the share kept in dir. A directory that holds none
// gives an empty share, for a build to fill in. One computed over another
// train file is an error: taking it would build a wrong index silently.
func loadBuildShare(dir, train string) (*buildShare, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if errors.Is(err, os.ErrNotExist) {
		return &buildShare{}, nil
	}
	if err != nil {
		return nil, err
	}
	var meta buildCacheMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, errors.Wrapf(err, "read %s", filepath.Join(dir, "meta.json"))
	}
	if meta.Train != train {
		return nil, errors.Errorf("%s was computed over another train set (%s), this one is %s", dir, meta.Train, train)
	}
	if meta.Vectors < 1 || meta.Dims < 1 || meta.Centroids < 1 || meta.Width < 1 {
		return nil, errors.Errorf("%s: meta.json describes nothing that can be read: %+v", dir, meta)
	}

	flat := make([]uint32, meta.Centroids*meta.Dims)
	if err := readUint32s(filepath.Join(dir, "centroids.f32"), flat); err != nil {
		return nil, err
	}
	values := make([]float32, len(flat))
	for i, bits := range flat {
		values[i] = math.Float32frombits(bits)
	}
	centroids := make([][]float32, meta.Centroids)
	for c := range centroids {
		centroids[c] = values[c*meta.Dims : (c+1)*meta.Dims : (c+1)*meta.Dims]
	}

	assigned := &assignment{
		primary: make([]uint32, meta.Vectors),
		probes:  make([]uint32, meta.Vectors*meta.Width),
		width:   meta.Width,
	}
	if err := readUint32s(filepath.Join(dir, "primary.u32"), assigned.primary); err != nil {
		return nil, err
	}
	if err := readUint32s(filepath.Join(dir, "probes.u32"), assigned.probes); err != nil {
		return nil, err
	}
	for i, p := range assigned.primary {
		if int(p) >= meta.Centroids {
			return nil, errors.Errorf("%s: vector %d is assigned to centroid %d of %d", dir, i, p, meta.Centroids)
		}
	}
	return &buildShare{
		vectors: meta.Vectors, postings: meta.Postings, probe: meta.Probe,
		centroids: centroids, assigned: assigned,
	}, nil
}

func writeCacheFile(path string, write func(w *bufio.Writer) error) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(file, 1<<20)
	if err := write(w); err != nil {
		file.Close()
		return errors.Wrapf(err, "write %s", path)
	}
	if err := w.Flush(); err != nil {
		file.Close()
		return errors.Wrapf(err, "write %s", path)
	}
	return file.Close()
}

// writeUint32s writes the values little-endian.
func writeUint32s(w io.Writer, values []uint32) error {
	raw := make([]byte, 4*min(len(values), 1<<18))
	for len(values) > 0 {
		n := min(len(values), len(raw)/4)
		for i, v := range values[:n] {
			binary.LittleEndian.PutUint32(raw[4*i:], v)
		}
		if _, err := w.Write(raw[:4*n]); err != nil {
			return err
		}
		values = values[n:]
	}
	return nil
}

// readUint32s fills the values from the file at path, which must hold exactly
// that many.
func readUint32s(path string, values []uint32) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != int64(4*len(values)) {
		return errors.Errorf("%s holds %d bytes, not the %d of %d values", path, info.Size(), 4*len(values), len(values))
	}
	r := bufio.NewReaderSize(file, 1<<20)
	raw := make([]byte, 4*min(len(values), 1<<18))
	for len(values) > 0 {
		n := min(len(values), len(raw)/4)
		if _, err := io.ReadFull(r, raw[:4*n]); err != nil {
			return errors.Wrapf(err, "read %s", path)
		}
		for i := range values[:n] {
			values[i] = binary.LittleEndian.Uint32(raw[4*i:])
		}
		values = values[n:]
	}
	return nil
}
