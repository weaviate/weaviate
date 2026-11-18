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

package postingedge

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// The first prototype that reads from disk, v4.0 (bench-report.md, "v4.0:
// primer prototipo que lee de disco"). Only the read path: an index built in
// batch is written to a directory and searched from it.
//
// A posting is three blocks, each under a key of its own in a bucket of the
// LSM store HFresh keeps its postings in (lsmkv, strategy Replace, one value
// per block): the members, with their doc ids and full codes; the edges
// leaving them; and, optionally, the halo, a short code of every distinct
// endpoint of those edges. A search asks for the blocks of all the postings
// of a round at once, and visits the postings in the order the index in
// memory visits them, so it returns what that index returns.
//
// What stays in memory is read from plain files when the index is opened: the
// centroids and their graph, the posting of every vector, the slot of every
// doc id and, unless the postings carry a halo, the code the endpoints are
// ranked by. The exact vectors of the rescore are in a file of their own, in
// posting order.
//
// Nothing of it is in the index: it lives with the tests, like the halo it
// is laid over.

const (
	diskBlockMembers byte = iota
	diskBlockEdges
	diskBlockHalo
)

const (
	diskFormatVersion = 1
	diskBucket        = "postingedge_postings"
	diskStoreDir      = "lsm"
	diskGraphDir      = "centroids"
	diskMetaFile      = "meta.json"
	diskMeanFile      = "mean.f32"
	diskLayoutFile    = "layout.u32"
	diskRoutingFile   = "routing.u64"
	diskCentroidFile  = "centroids.f32"
	diskVectorFile    = "vectors.f32"

	// diskNoSlot marks a doc id the index does not hold.
	diskNoSlot = math.MaxUint32
	// diskKeyBytes is the posting, big-endian so that the blocks of a
	// posting are neighbors in the segment, and the block.
	diskKeyBytes = 5
)

// diskMeta describes a written index. It is the first file a reader opens.
type diskMeta struct {
	Version  int    `json:"version"`
	Distance string `json:"distance"`
	Dims     int    `json:"dims"`
	Vectors  int    `json:"vectors"`
	Postings int    `json:"postings"`
	Edges    int    `json:"edges"`
	// DocIDs is the length of the table of slots by doc id: one more than
	// the largest doc id.
	DocIDs int `json:"docIDs"`
	// ScanCodeWords is the length of the full code the members carry.
	ScanCodeWords int `json:"scanCodeWords"`
	// RoutingBits is the width of the code the endpoints of the edges are
	// ranked by, 0 for the full code, and RoutingCodeWords its length.
	RoutingBits      int `json:"routingBits"`
	RoutingCodeWords int `json:"routingCodeWords"`
	// Halo tells that the routing codes are in a block of every posting and
	// not in memory. HaloEntries counts them over all the postings.
	Halo        bool `json:"halo"`
	HaloEntries int  `json:"haloEntries"`
	// MemberBytes, EdgeBytes and HaloBytes are what the blocks of each kind
	// take, summed over the postings, before what the store adds.
	MemberBytes int64 `json:"memberBytes"`
	EdgeBytes   int64 `json:"edgeBytes"`
	HaloBytes   int64 `json:"haloBytes"`
}

func diskKey(p uint32, block byte) []byte {
	key := make([]byte, diskKeyBytes)
	binary.BigEndian.PutUint32(key, p)
	key[4] = block
	return key
}

// diskCodecs tells the quantizer behind the codes of a state and the width of
// the code its endpoints are ranked by, 0 for the full code. The members of a
// written posting always carry the full code.
func diskCodecs(st *state) (*compressionhelpers.CenteredBinaryRotationalQuantizer, int, error) {
	scan := st.quantizer
	if st.scanQuantizer != nil {
		scan = st.scanQuantizer
	}
	full, ok := scan.(fullCodec)
	if !ok {
		return nil, 0, errors.Errorf("the postings carry codes of %T, the format writes the full code", scan)
	}
	if st.scanQuantizer == nil {
		return full.CenteredBinaryRotationalQuantizer, 0, nil
	}
	if _, ok := st.quantizer.(cappedCodec); !ok {
		return nil, 0, errors.Errorf("the endpoints are ranked by %T, expected a capped code", st.quantizer)
	}
	// A capped code is a header word and its sign bits.
	return full.CenteredBinaryRotationalQuantizer, (st.side.codeWords - 1) * 64, nil
}

// encodeMembers lays out the members block of a posting: the doc ids, then
// the codes.
func encodeMembers(post *posting) []byte {
	out := make([]byte, 0, 8*(len(post.docIDs)+len(post.codes)))
	for _, id := range post.docIDs {
		out = binary.LittleEndian.AppendUint64(out, id)
	}
	for _, word := range post.codes {
		out = binary.LittleEndian.AppendUint64(out, word)
	}
	return out
}

// encodeEdges lays out the edges block of a posting: the degree of every
// member in a byte, then the endpoint slots of all the edges.
func encodeEdges(post *posting) ([]byte, error) {
	out := make([]byte, 0, len(post.docIDs)+4*len(post.edges))
	for i := range post.docIDs {
		degree := len(post.edgesOf(i))
		if degree > math.MaxUint8 {
			return nil, errors.Errorf("member %d has %d edges, the format holds %d", i, degree, math.MaxUint8)
		}
		out = append(out, byte(degree))
	}
	for _, endpoint := range post.edges {
		out = binary.LittleEndian.AppendUint32(out, endpoint)
	}
	return out, nil
}

// encodeHalo lays out the halo block of a posting: the slots of the distinct
// endpoints of its edges in ascending order, then their codes in the sidecar,
// in that order. It tells how many entries it holds.
func encodeHalo(post *posting, side *sidecar) ([]byte, int) {
	endpoints := slices.Clone(post.edges)
	slices.Sort(endpoints)
	endpoints = slices.Compact(endpoints)
	out := make([]byte, 0, len(endpoints)*(4+8*side.codeWords))
	for _, slot := range endpoints {
		out = binary.LittleEndian.AppendUint32(out, slot)
	}
	for _, slot := range endpoints {
		for _, word := range side.code(slot) {
			out = binary.LittleEndian.AppendUint64(out, word)
		}
	}
	return out, len(endpoints)
}

// writeArray writes values of size bytes each through put, buffered.
func writeArray[T any](path string, size int, put func([]byte, T), arrays ...[]T) (err error) {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}()
	w := bufio.NewWriterSize(file, 1<<20)
	buf := make([]byte, size)
	for _, array := range arrays {
		for _, value := range array {
			put(buf, value)
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}
	}
	return w.Flush()
}

func putFloat32(buf []byte, value float32) {
	binary.LittleEndian.PutUint32(buf, math.Float32bits(value))
}

// readArray reads a file of values of size bytes each, n of them.
func readArray[T any](path string, size, n int, get func([]byte) T) ([]T, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) != n*size {
		return nil, errors.Errorf("%s holds %d bytes, expected %d", filepath.Base(path), len(raw), n*size)
	}
	out := make([]T, n)
	for i := range out {
		out[i] = get(raw[i*size:])
	}
	return out, nil
}

func getFloat32(buf []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(buf))
}

// openDiskStore opens the LSM store of an index and its bucket of postings.
// With pread the store reads every block with one positional read of its
// exact size, which is the only mode it counts reads in; otherwise it copies
// the block out of the mapped segment.
func openDiskStore(ctx context.Context, logger logrus.FieldLogger, dir string, metrics *lsmkv.Metrics, pread bool,
) (*lsmkv.Store, *lsmkv.Bucket, error) {
	root := filepath.Join(dir, diskStoreDir)
	store, err := lsmkv.New(root, root, logger, metrics, nil, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, nil, errors.Wrap(err, "open the store")
	}
	err = store.CreateOrLoadBucket(ctx, diskBucket, lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithPread(pread))
	if err != nil {
		_ = store.Shutdown(ctx)
		return nil, nil, errors.Wrap(err, "open the bucket of postings")
	}
	return store, store.Bucket(diskBucket), nil
}

// openCentroidGraph opens the centroid graph of an index on disk: the graph
// of newCentroidIndex with a commit log under dir. A directory without one
// gives an empty graph that logs what is added to it, and one with a log
// gives back the graph that was logged, link by link. A graph built again
// from the same centroids would not be the same graph.
func openCentroidGraph(ctx context.Context, logger logrus.FieldLogger, dir string, vectors [][]float32,
) (*centroidIndex, error) {
	index := &centroidIndex{vectors: vectors}
	vectorForID := func(_ context.Context, id uint64) ([]float32, error) {
		if id >= uint64(len(index.vectors)) {
			return nil, storobj.NewErrNotFoundf(id, "centroid out of range")
		}
		return index.vectors[id], nil
	}
	graph, err := hnsw.New(hnsw.Config{
		RootPath: dir,
		ID:       "centroids",
		Logger:   logger,
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(dir, "centroids", logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		DistanceProvider: distancer.NewL2SquaredProvider(),
		VectorForIDThunk: vectorForID,
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice,
			_ common.BucketView,
		) ([]float32, error) {
			vec, err := vectorForID(ctx, id)
			if err != nil {
				return nil, err
			}
			container.Slice = append(container.Slice[:0], vec...)
			return container.Slice, nil
		},
		GetViewThunk: func() common.BucketView { return noopBucketView{} },
		HFreshMode:   true,
	}, centroidUserConfig(centroidEF), cyclemanager.NewCallbackGroupNoop(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "open the centroid graph")
	}
	graph.PostStartup(ctx)
	index.graph = graph
	return index, nil
}

// writeDiskIndex writes a built index to an empty directory. st is the state
// to write: that of the build, or one laid over it with another code for the
// endpoints (haloState) or other edges (replaceEdges). With halo the code of
// the endpoints goes into a block of every posting, and otherwise into the
// file a reader loads into memory. vectorOf gives the vector of a doc id as
// the index reads it for the rescore.
func writeDiskIndex(ctx context.Context, logger logrus.FieldLogger, dir string, st *state, halo bool,
	provider distancer.Provider, vectorOf func(docID uint64) []float32,
) (*diskMeta, error) {
	if st == nil {
		return nil, ErrNotBuilt
	}
	store := st.postings
	if store.lists != nil {
		return nil, errors.New("the index is keyed by doc id: the format writes an index built in batch")
	}
	quantizer, routingBits, err := diskCodecs(st)
	if err != nil {
		return nil, err
	}
	if entries, err := os.ReadDir(dir); err != nil {
		return nil, err
	} else if len(entries) > 0 {
		return nil, errors.Errorf("%s is not empty", dir)
	}
	n, postings := store.vectors(), store.postings()
	if n == 0 {
		return nil, errors.New("no vector to write")
	}
	meta := &diskMeta{
		Version: diskFormatVersion, Distance: provider.Type(), Dims: st.dims, Vectors: n, Postings: postings,
		Edges: len(store.edges), ScanCodeWords: store.codeWords, RoutingBits: routingBits,
		RoutingCodeWords: st.side.codeWords, Halo: halo,
	}

	// The slot of every doc id, which is where its exact vector is.
	maxID := slices.Max(store.docIDs)
	if maxID >= uint64(4*n+1024) {
		return nil, errors.Errorf("doc ids up to %d for %d vectors: the prototype keeps a table by doc id", maxID, n)
	}
	slotOf := make([]uint32, maxID+1)
	for i := range slotOf {
		slotOf[i] = diskNoSlot
	}
	for slot, id := range store.docIDs {
		if slotOf[id] != diskNoSlot {
			return nil, errors.Errorf("doc id %d is in the index twice", id)
		}
		slotOf[id] = uint32(slot)
	}
	meta.DocIDs = len(slotOf)

	path := func(name string) string { return filepath.Join(dir, name) }
	if err := writeArray(path(diskMeanFile), 4, putFloat32, quantizer.Data().Mean); err != nil {
		return nil, errors.Wrap(err, "write the mean of the quantizer")
	}
	err = writeArray(path(diskLayoutFile), 4, binary.LittleEndian.PutUint32, store.start, st.side.postingOf, slotOf)
	if err != nil {
		return nil, errors.Wrap(err, "write the layout")
	}
	if !halo {
		if err := writeArray(path(diskRoutingFile), 8, binary.LittleEndian.PutUint64, st.side.codes); err != nil {
			return nil, errors.Wrap(err, "write the routing codes")
		}
	}
	if err := writeArray(path(diskCentroidFile), 4, putFloat32, st.centroids.vectors...); err != nil {
		return nil, errors.Wrap(err, "write the centroids")
	}

	// The graph over the centroids is built here, with a commit log, and
	// not copied from the index, whose graph logs nothing.
	graphDir := path(diskGraphDir)
	if err := os.Mkdir(graphDir, 0o755); err != nil {
		return nil, err
	}
	graph, err := openCentroidGraph(ctx, logger, graphDir, st.centroids.vectors)
	if err != nil {
		return nil, err
	}
	for id, vector := range st.centroids.vectors {
		if err := graph.graph.Add(ctx, uint64(id), vector); err != nil {
			_ = graph.shutdown(ctx)
			return nil, errors.Wrapf(err, "add centroid %d", id)
		}
	}
	if err := graph.graph.Flush(); err != nil {
		_ = graph.shutdown(ctx)
		return nil, errors.Wrap(err, "flush the centroid graph")
	}
	if err := graph.shutdown(ctx); err != nil {
		return nil, errors.Wrap(err, "close the centroid graph")
	}

	// The exact vectors, in posting order.
	err = func() (err error) {
		file, err := os.Create(path(diskVectorFile))
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}()
		w := bufio.NewWriterSize(file, 1<<20)
		buf := make([]byte, 4*st.dims)
		for _, id := range store.docIDs {
			vector := vectorOf(id)
			if len(vector) != st.dims {
				return errors.Errorf("vector of doc %d has length %d, expected %d", id, len(vector), st.dims)
			}
			for i, value := range vector {
				putFloat32(buf[4*i:], value)
			}
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}
		return w.Flush()
	}()
	if err != nil {
		return nil, errors.Wrap(err, "write the vectors")
	}

	// The blocks: one memtable, flushed once, is one segment.
	lsm, bucket, err := openDiskStore(ctx, logger, dir, nil, false)
	if err != nil {
		return nil, err
	}
	err = func() error {
		for p := 0; p < postings; p++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			post := store.load(uint32(p))
			members := encodeMembers(&post)
			edges, err := encodeEdges(&post)
			if err != nil {
				return errors.Wrapf(err, "posting %d", p)
			}
			// The store keeps the slices it is given until the flush.
			if err := bucket.Put(diskKey(uint32(p), diskBlockMembers), members); err != nil {
				return errors.Wrapf(err, "put the members of posting %d", p)
			}
			if err := bucket.Put(diskKey(uint32(p), diskBlockEdges), edges); err != nil {
				return errors.Wrapf(err, "put the edges of posting %d", p)
			}
			meta.MemberBytes += int64(len(members))
			meta.EdgeBytes += int64(len(edges))
			if !halo {
				continue
			}
			block, entries := encodeHalo(&post, st.side)
			if err := bucket.Put(diskKey(uint32(p), diskBlockHalo), block); err != nil {
				return errors.Wrapf(err, "put the halo of posting %d", p)
			}
			meta.HaloBytes += int64(len(block))
			meta.HaloEntries += entries
		}
		return errors.Wrap(bucket.FlushAndSwitch(), "flush the postings")
	}()
	if shutdownErr := lsm.Shutdown(ctx); err == nil {
		err = errors.Wrap(shutdownErr, "close the store")
	}
	if err != nil {
		return nil, err
	}

	raw, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, err
	}
	// The meta goes last: a directory without it is not an index.
	if err := os.WriteFile(path(diskMetaFile), raw, 0o644); err != nil {
		return nil, errors.Wrap(err, "write the meta")
	}
	return meta, nil
}

// diskIndex is an index opened from the directory writeDiskIndex wrote.
type diskIndex struct {
	dir    string
	meta   diskMeta
	logger logrus.FieldLogger
	// ix holds the search tunables and rescores as the index does, reading
	// the vectors through readVector. It is never built.
	ix *Index
	// st has what a search keeps in memory. Its posting store only knows
	// the range of slots of every posting.
	st      *state
	lsm     *lsmkv.Store
	bucket  *lsmkv.Bucket
	slotOf  []uint32 // by doc id
	vectors *os.File
	rounds  sync.Pool // *diskRound
}

// openDiskIndex opens a written index. uc carries the search tunables;
// everything about the build comes from the directory. metrics and pread are
// those of openDiskStore.
func openDiskIndex(ctx context.Context, logger logrus.FieldLogger, dir string, provider distancer.Provider,
	uc UserConfig, metrics *lsmkv.Metrics, pread bool,
) (_ *diskIndex, err error) {
	path := func(name string) string { return filepath.Join(dir, name) }
	raw, err := os.ReadFile(path(diskMetaFile))
	if err != nil {
		return nil, errors.Wrap(err, "read the meta")
	}
	d := &diskIndex{dir: dir, logger: logger}
	if err := json.Unmarshal(raw, &d.meta); err != nil {
		return nil, errors.Wrap(err, "parse the meta")
	}
	meta := &d.meta
	switch {
	case meta.Version != diskFormatVersion:
		return nil, errors.Errorf("format version %d, this reader reads %d", meta.Version, diskFormatVersion)
	case meta.Distance != provider.Type():
		return nil, errors.Errorf("the index was written for distance %q, opened with %q", meta.Distance, provider.Type())
	case meta.Dims <= 0 || meta.Vectors <= 0 || meta.Postings <= 0:
		return nil, errors.Errorf("meta of an empty index: %d dims, %d vectors, %d postings", meta.Dims, meta.Vectors, meta.Postings)
	}
	// Everything opened so far is closed if a later step fails.
	defer func() {
		if err != nil {
			_ = d.close(ctx)
		}
	}()

	mean, err := readArray(path(diskMeanFile), 4, meta.Dims, getFloat32)
	if err != nil {
		return nil, errors.Wrap(err, "read the mean of the quantizer")
	}
	quantizer, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(meta.Dims,
		compressionhelpers.DefaultFastRotationSeed, mean, provider)
	if err != nil {
		return nil, errors.Wrap(err, "restore the quantizer")
	}
	scan := fullCodec{quantizer}
	var routing codec = scan
	if meta.RoutingBits > 0 {
		if routing, err = scan.withBits(meta.RoutingBits); err != nil {
			return nil, err
		}
	}

	layout, err := readArray(path(diskLayoutFile), 4, meta.Postings+1+meta.Vectors+meta.DocIDs, binary.LittleEndian.Uint32)
	if err != nil {
		return nil, errors.Wrap(err, "read the layout")
	}
	start := layout[: meta.Postings+1 : meta.Postings+1]
	postingOf := layout[meta.Postings+1 : meta.Postings+1+meta.Vectors : meta.Postings+1+meta.Vectors]
	d.slotOf = layout[meta.Postings+1+meta.Vectors:]
	if start[0] != 0 || int(start[meta.Postings]) != meta.Vectors {
		return nil, errors.Errorf("the postings span the slots [%d, %d), the index has %d", start[0], start[meta.Postings], meta.Vectors)
	}
	side := &sidecar{codeWords: meta.RoutingCodeWords, postingOf: postingOf}
	if !meta.Halo {
		side.codes, err = readArray(path(diskRoutingFile), 8, meta.Vectors*meta.RoutingCodeWords, binary.LittleEndian.Uint64)
		if err != nil {
			return nil, errors.Wrap(err, "read the routing codes")
		}
	}

	flat, err := readArray(path(diskCentroidFile), 4, meta.Postings*meta.Dims, getFloat32)
	if err != nil {
		return nil, errors.Wrap(err, "read the centroids")
	}
	centroids := make([][]float32, meta.Postings)
	for p := range centroids {
		centroids[p] = flat[p*meta.Dims : (p+1)*meta.Dims : (p+1)*meta.Dims]
	}
	graph, err := openCentroidGraph(ctx, logger, path(diskGraphDir), centroids)
	if err != nil {
		return nil, err
	}
	postings := meta.Postings
	d.st = &state{
		dims:      meta.Dims,
		quantizer: routing,
		centroids: graph,
		postings:  &postingStore{codeWords: meta.ScanCodeWords, start: start},
		side:      side,
		visited:   visited.NewPool(meta.Vectors),
	}
	if meta.RoutingBits > 0 {
		d.st.scanQuantizer = scan
	}
	d.st.scratch.New = func() any { return newSearchScratch(postings) }
	d.rounds.New = func() any { return &diskRound{} }

	if d.vectors, err = os.Open(path(diskVectorFile)); err != nil {
		return nil, errors.Wrap(err, "open the vectors")
	}
	info, err := d.vectors.Stat()
	if err != nil {
		return nil, err
	}
	if want := int64(meta.Vectors) * int64(meta.Dims) * 4; info.Size() != want {
		return nil, errors.Errorf("the vectors take %d bytes, expected %d", info.Size(), want)
	}

	if d.lsm, d.bucket, err = openDiskStore(ctx, logger, dir, metrics, pread); err != nil {
		return nil, err
	}
	d.ix, err = New(Config{
		ID:                           "disk",
		RootPath:                     dir,
		Logger:                       logger,
		DistanceProvider:             provider,
		TempVectorForIDWithViewThunk: d.readVector,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
	}, uc)
	if err != nil {
		return nil, errors.Wrap(err, "configure the search")
	}
	return d, nil
}

// close releases what the index holds. It must not run next to a search.
func (d *diskIndex) close(ctx context.Context) error {
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	if d.ix != nil {
		keep(d.ix.Shutdown(ctx))
	}
	if d.st != nil && d.st.centroids != nil {
		keep(d.st.centroids.shutdown(ctx))
	}
	if d.lsm != nil {
		keep(d.lsm.Shutdown(ctx))
	}
	if d.vectors != nil {
		keep(d.vectors.Close())
	}
	return first
}

// readVector reads the exact vector of a doc from its slot of the vectors
// file, with one positional read.
func (d *diskIndex) readVector(_ context.Context, docID uint64, container *common.VectorSlice, _ common.BucketView,
) ([]float32, error) {
	if docID >= uint64(len(d.slotOf)) || d.slotOf[docID] == diskNoSlot {
		return nil, storobj.NewErrNotFoundf(docID, "the index does not hold it")
	}
	dims := d.meta.Dims
	if cap(container.Buff) < 4*dims {
		container.Buff = make([]byte, 4*dims)
	}
	buf := container.Buff[:4*dims]
	if _, err := d.vectors.ReadAt(buf, int64(d.slotOf[docID])*int64(4*dims)); err != nil {
		return nil, errors.Wrapf(err, "read the vector of doc %d", docID)
	}
	if cap(container.Slice) < dims {
		container.Slice = make([]float32, dims)
	}
	container.Slice = container.Slice[:dims]
	for i := range container.Slice {
		container.Slice[i] = getFloat32(buf[4*i:])
	}
	return container.Slice, nil
}

// diskPosting is one posting fetched from the store: its blocks decoded into
// buffers a search reuses.
type diskPosting struct {
	post      posting
	docIDs    []uint64
	codes     []uint64
	edges     []uint32
	edgeStart []uint32
	haloSlots []uint32
	haloCodes []uint64
	// bytes is what the store returned for its blocks.
	bytes [3]int
}

// diskRound holds the postings of one round, fetched together.
type diskRound struct {
	postings []diskPosting
}

func resized[T any](buf []T, n int) []T {
	if cap(buf) < n {
		return make([]T, n)
	}
	return buf[:n]
}

// block reads one block of a posting from the store.
func (d *diskIndex) block(p uint32, block byte) ([]byte, error) {
	value, err := d.bucket.Get(diskKey(p, block))
	if err != nil {
		return nil, errors.Wrapf(err, "read block %d of posting %d", block, p)
	}
	if value == nil {
		return nil, errors.Errorf("posting %d has no block %d", p, block)
	}
	return value, nil
}

func (d *diskIndex) fetchMembers(p uint32, dp *diskPosting) error {
	value, err := d.block(p, diskBlockMembers)
	if err != nil {
		return err
	}
	words := d.meta.ScanCodeWords
	n := int(d.st.postings.start[p+1] - d.st.postings.start[p])
	if len(value) != n*8*(1+words) {
		return errors.Errorf("the members of posting %d take %d bytes, expected %d for %d members", p, len(value), n*8*(1+words), n)
	}
	dp.docIDs, dp.codes = resized(dp.docIDs, n), resized(dp.codes, n*words)
	for i := range dp.docIDs {
		dp.docIDs[i] = binary.LittleEndian.Uint64(value[8*i:])
	}
	codes := value[8*n:]
	for i := range dp.codes {
		dp.codes[i] = binary.LittleEndian.Uint64(codes[8*i:])
	}
	dp.bytes[diskBlockMembers] = len(value)
	return nil
}

func (d *diskIndex) fetchEdges(p uint32, dp *diskPosting) error {
	value, err := d.block(p, diskBlockEdges)
	if err != nil {
		return err
	}
	n := int(d.st.postings.start[p+1] - d.st.postings.start[p])
	if len(value) < n || (len(value)-n)%4 != 0 {
		return errors.Errorf("the edges of posting %d take %d bytes, with %d members", p, len(value), n)
	}
	dp.edgeStart, dp.edges = resized(dp.edgeStart, n+1), resized(dp.edges, (len(value)-n)/4)
	dp.edgeStart[0] = 0
	for i, degree := range value[:n] {
		dp.edgeStart[i+1] = dp.edgeStart[i] + uint32(degree)
	}
	if int(dp.edgeStart[n]) != len(dp.edges) {
		return errors.Errorf("the degrees of posting %d add up to %d edges, its block holds %d", p, dp.edgeStart[n], len(dp.edges))
	}
	endpoints := value[n:]
	for i := range dp.edges {
		dp.edges[i] = binary.LittleEndian.Uint32(endpoints[4*i:])
	}
	dp.bytes[diskBlockEdges] = len(value)
	return nil
}

func (d *diskIndex) fetchHalo(p uint32, dp *diskPosting) error {
	value, err := d.block(p, diskBlockHalo)
	if err != nil {
		return err
	}
	words := d.meta.RoutingCodeWords
	if len(value)%(4+8*words) != 0 {
		return errors.Errorf("the halo of posting %d takes %d bytes, not a number of entries of %d", p, len(value), 4+8*words)
	}
	entries := len(value) / (4 + 8*words)
	dp.haloSlots, dp.haloCodes = resized(dp.haloSlots, entries), resized(dp.haloCodes, entries*words)
	for i := range dp.haloSlots {
		dp.haloSlots[i] = binary.LittleEndian.Uint32(value[4*i:])
	}
	codes := value[4*entries:]
	for i := range dp.haloCodes {
		dp.haloCodes[i] = binary.LittleEndian.Uint64(codes[8*i:])
	}
	dp.bytes[diskBlockHalo] = len(value)
	return nil
}

// fetch reads the blocks of the given postings, all at once, and leaves each
// posting ready to be visited.
func (d *diskIndex) fetch(round *diskRound, ids []scored) error {
	if cap(round.postings) < len(ids) {
		round.postings = append(round.postings[:cap(round.postings)], make([]diskPosting, len(ids)-cap(round.postings))...)
	}
	round.postings = round.postings[:len(ids)]
	eg := enterrors.NewErrorGroupWrapper(d.logger)
	for i, pick := range ids {
		p, dp := uint32(pick.id), &round.postings[i]
		dp.bytes = [3]int{}
		eg.Go(func() error { return d.fetchMembers(p, dp) })
		eg.Go(func() error { return d.fetchEdges(p, dp) })
		if d.meta.Halo {
			eg.Go(func() error { return d.fetchHalo(p, dp) })
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	for i, pick := range ids {
		dp := &round.postings[i]
		dp.post = posting{
			first:     d.st.postings.start[pick.id],
			docIDs:    dp.docIDs,
			codes:     dp.codes,
			edges:     dp.edges,
			edgeStart: dp.edgeStart,
		}
	}
	return nil
}

// scoreHalo estimates the distance to the given endpoints from the codes in
// the halo of the posting, as the sidecar does from its own.
func (dp *diskPosting) scoreHalo(estimator codeEstimator, words int, slots []uint32, dists []float32) error {
	for i, slot := range slots {
		at, found := slices.BinarySearch(dp.haloSlots, slot)
		if !found {
			return errors.Errorf("endpoint %d is not in the halo of the posting that leads to it", slot)
		}
		dist, err := estimator.Distance(dp.haloCodes[at*words : (at+1)*words])
		if err != nil {
			return errors.Wrapf(err, "estimate distance to slot %d", slot)
		}
		dists[i] = dist
	}
	return nil
}

// diskSearchStats is what a search from disk read and where its time went.
type diskSearchStats struct {
	// Work, when set, collects the stats of the search of the index, which
	// costs a second pass over every posting.
	Work *SearchStats
	// Blocks and BlockBytes count the blocks the store returned, and
	// MemberBytes, EdgeBytes and HaloBytes split the bytes by block.
	Blocks      int
	BlockBytes  int
	MemberBytes int
	EdgeBytes   int
	HaloBytes   int
	// Postings and Rounds count the postings loaded and the rounds they
	// came in, the entry postings aside.
	Postings int
	Rounds   int
	// Rescored counts the exact vectors read, one read each.
	Rescored     int
	CentroidTime time.Duration
	FetchTime    time.Duration
	VisitTime    time.Duration
	RescoreTime  time.Duration
}

// search is the search of the index (Index.searchFrom) with the postings
// fetched from disk: the same entry, the same rounds and the same visits, in
// the same order. Every round is fetched whole before any of it is visited.
func (d *diskIndex) search(ctx context.Context, vector []float32, k int, stats *diskSearchStats,
) ([]uint64, []float32, error) {
	ix, st := d.ix, d.st
	if k < 1 {
		return nil, nil, errors.Errorf("k must be positive, got %d", k)
	}
	if len(vector) != st.dims {
		return nil, nil, errors.Wrapf(common.ErrWrongDimensions,
			"query has length %d, the index holds vectors of length %d", len(vector), st.dims)
	}
	if err := checkValues(vector); err != nil {
		return nil, nil, errors.Wrap(err, "query")
	}
	query := ix.normalized(vector)
	ef := max(int(ix.ef.Load()), k)
	rescoreLimit := int(ix.rescoreLimit.Load())
	entries := int(ix.entryPostings.Load())
	batch := int(ix.loadBatch.Load())
	margin := math.Float32frombits(ix.sourceMargin.Load())
	if stats == nil {
		stats = &diskSearchStats{}
	}

	sc := st.borrowScratch()
	defer st.returnScratch(sc)
	round := d.rounds.Get().(*diskRound)
	defer d.rounds.Put(round)

	poolSize := max(rescoreLimit, k)
	sc.pool.reset(poolSize)
	stop := &sc.pool
	if ef != poolSize {
		sc.stop.reset(ef)
		stop = &sc.stop
	}
	search := st.newPostingSearch(sc, query, stop, margin, stats.Work)
	var visiting *diskPosting
	if d.meta.Halo {
		words := d.meta.RoutingCodeWords
		search.scoreEndpoints = func(_ *posting, slots []uint32, dists []float32) error {
			return visiting.scoreHalo(search.estimator, words, slots, dists)
		}
	}

	clock := time.Now()
	entryIDs, err := st.centroids.nearest(ctx, query, entries)
	if err != nil {
		return nil, nil, err
	}
	stats.CentroidTime = time.Since(clock)

	// load fetches a set of postings together and visits them in order.
	// All of them are marked before any is visited, so that edges between
	// postings of the same round are not scored.
	load := func(picks []scored) error {
		for _, pick := range picks {
			sc.markLoaded(uint32(pick.id))
		}
		clock := time.Now()
		if err := d.fetch(round, picks); err != nil {
			return err
		}
		stats.FetchTime += time.Since(clock)
		clock = time.Now()
		for i, pick := range picks {
			visiting = &round.postings[i]
			if err := search.visit(uint32(pick.id), &visiting.post); err != nil {
				return err
			}
			stats.MemberBytes += visiting.bytes[diskBlockMembers]
			stats.EdgeBytes += visiting.bytes[diskBlockEdges]
			stats.HaloBytes += visiting.bytes[diskBlockHalo]
			stats.Blocks += 2
			if d.meta.Halo {
				stats.Blocks++
			}
		}
		stats.VisitTime += time.Since(clock)
		stats.Postings += len(picks)
		return nil
	}

	sc.batch = sc.batch[:0]
	for _, p := range entryIDs {
		sc.batch = append(sc.batch, scored{id: p})
	}
	if err := load(sc.batch); err != nil {
		return nil, nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		sc.selectBatch(batch)
		if len(sc.batch) == 0 || sc.batch[0].dist >= stop.worst() {
			break
		}
		stats.Rounds++
		if err := load(sc.batch); err != nil {
			return nil, nil, err
		}
	}
	stats.BlockBytes = stats.MemberBytes + stats.EdgeBytes + stats.HaloBytes
	if stats.Work != nil {
		// The search of the index counts its rounds where it runs them.
		stats.Work.Rounds = stats.Rounds
	}

	candidates := sc.pool.sorted()
	if rescoreLimit == 0 {
		return topResults(candidates, k)
	}
	clock = time.Now()
	if err := ix.rescore(ctx, query, candidates); err != nil {
		return nil, nil, err
	}
	sc.result.reset(k)
	for _, cand := range candidates {
		sc.result.push(cand.id, cand.dist)
	}
	stats.Rescored = len(candidates)
	stats.RescoreTime = time.Since(clock)
	if stats.Work != nil {
		stats.Work.Rescored = len(candidates)
	}
	return topResults(sc.result.sorted(), k)
}
