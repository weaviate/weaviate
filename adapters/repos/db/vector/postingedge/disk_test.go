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
	// ScanInMemory tells that a posting has no members block (v4.1): its
	// members are scanned by the routing codes, which are in memory, and
	// their doc ids come from the table of slots. A posting on disk is then
	// its edges, and its halo if it has one.
	ScanInMemory bool `json:"scanInMemory,omitempty"`
	// MemberBytes, EdgeBytes and HaloBytes are what the blocks of each kind
	// take, summed over the postings, before what the store adds.
	MemberBytes int64 `json:"memberBytes"`
	EdgeBytes   int64 `json:"edgeBytes"`
	HaloBytes   int64 `json:"haloBytes"`
}

// sketchScanState returns the state of an index whose members are scanned by
// the code its endpoints are ranked by (v4.1): the state haloState gives, with
// the sketches of its sidecar as the codes of its postings too. One array
// serves both, by slot, so the scan of a posting reads a contiguous run of it.
// The edges, the centroid graph and the posting map are those of the state.
func sketchScanState(sketched *state) (*state, error) {
	switch {
	case sketched == nil:
		return nil, ErrNotBuilt
	case sketched.postings.lists != nil:
		return nil, errors.New("the index is keyed by doc id: the sketches are laid over an index built in batch")
	case sketched.scanQuantizer == nil:
		return nil, errors.New("the postings are scanned by the code of the endpoints already")
	}
	store := *sketched.postings
	store.codeWords, store.codes = sketched.side.codeWords, sketched.side.codes
	next := &state{
		dims:      sketched.dims,
		quantizer: sketched.quantizer,
		centroids: sketched.centroids,
		postings:  &store,
		side:      sketched.side,
		visited:   sketched.visited,
		build:     sketched.build,
	}
	next.scratch.New = sketched.scratch.New
	return next, nil
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

// writeDiskVectors writes the exact vector of every doc id, in the order
// given, which is the order of the slots.
func writeDiskVectors(path string, dims int, docIDs []uint64, vectorOf func(docID uint64) []float32) (err error) {
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
	buf := make([]byte, 4*dims)
	for _, id := range docIDs {
		vector := vectorOf(id)
		if len(vector) != dims {
			return errors.Errorf("vector of doc %d has length %d, expected %d", id, len(vector), dims)
		}
		for i, value := range vector {
			putFloat32(buf[4*i:], value)
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return w.Flush()
}

// diskWriteOptions says how a state is written.
type diskWriteOptions struct {
	// Halo puts the code of the endpoints into a block of every posting.
	// Without it the code goes into the file a reader loads into memory.
	Halo bool
	// ScanInMemory writes no members block: the state is one of
	// sketchScanState, whose members are scanned by the codes in memory.
	ScanInMemory bool
	// ShareFrom is the directory of an index written from the same build,
	// whose vectors are in the same slots and whose centroids are the same.
	// The vectors are then a hard link to its file, which is by far the
	// largest of an index, and the centroid graph a copy of its graph, so
	// that the two indexes enter a search through the same postings. Empty,
	// the index gets vectors and a graph of its own.
	ShareFrom string
}

// writeDiskIndex writes a built index to an empty directory. st is the state
// to write: that of the build, or one laid over it with another code for the
// endpoints (haloState) or other edges (replaceEdges). vectorOf gives the
// vector of a doc id as the index reads it for the rescore.
func writeDiskIndex(ctx context.Context, logger logrus.FieldLogger, dir string, st *state, opts diskWriteOptions,
	provider distancer.Provider, vectorOf func(docID uint64) []float32,
) (*diskMeta, error) {
	halo := opts.Halo
	if st == nil {
		return nil, ErrNotBuilt
	}
	store := st.postings
	if store.lists != nil {
		return nil, errors.New("the index is keyed by doc id: the format writes an index built in batch")
	}
	quantizer, routingBits, sketchScan, err := diskCodecs(st)
	if err != nil {
		return nil, err
	}
	switch {
	case sketchScan && !opts.ScanInMemory:
		return nil, errors.New("the postings carry the code of the endpoints: the format keeps it in memory only (ScanInMemory)")
	case opts.ScanInMemory && !sketchScan:
		return nil, errors.New("ScanInMemory writes a state whose postings are scanned by the code of its endpoints (sketchScanState)")
	case opts.ScanInMemory && halo:
		return nil, errors.New("ScanInMemory keeps the code of the endpoints in memory, a halo keeps it in the postings")
	case opts.ScanInMemory && (store.codeWords != st.side.codeWords || len(store.codes) != len(st.side.codes) ||
		(len(store.codes) > 0 && &store.codes[0] != &st.side.codes[0])):
		return nil, errors.New("ScanInMemory: the codes of the postings are not the array of the sidecar")
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
		RoutingCodeWords: st.side.codeWords, Halo: halo, ScanInMemory: opts.ScanInMemory,
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
	if opts.ShareFrom != "" {
		if err := os.CopyFS(graphDir, os.DirFS(filepath.Join(opts.ShareFrom, diskGraphDir))); err != nil {
			return nil, errors.Wrap(err, "copy the centroid graph")
		}
	} else if err := writeCentroidGraph(ctx, logger, graphDir, st.centroids.vectors); err != nil {
		return nil, err
	}

	// The exact vectors, in posting order.
	if opts.ShareFrom != "" {
		err = os.Link(filepath.Join(opts.ShareFrom, diskVectorFile), path(diskVectorFile))
	} else {
		err = writeDiskVectors(path(diskVectorFile), st.dims, store.docIDs, vectorOf)
	}
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
			edges, err := encodeEdges(&post)
			if err != nil {
				return errors.Wrapf(err, "posting %d", p)
			}
			// The store keeps the slices it is given until the flush.
			if !opts.ScanInMemory {
				members := encodeMembers(&post)
				if err := bucket.Put(diskKey(uint32(p), diskBlockMembers), members); err != nil {
					return errors.Wrapf(err, "put the members of posting %d", p)
				}
				meta.MemberBytes += int64(len(members))
			}
			if err := bucket.Put(diskKey(uint32(p), diskBlockEdges), edges); err != nil {
				return errors.Wrapf(err, "put the edges of posting %d", p)
			}
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
	st     *state
	lsm    *lsmkv.Store
	bucket *lsmkv.Bucket
	slotOf []uint32 // by doc id
	// docIDs is the doc id of every slot, in an index whose members are
	// scanned from memory: its postings on disk do not bring them.
	docIDs  []uint64
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
	case meta.ScanInMemory && (meta.Halo || meta.RoutingBits == 0 || meta.ScanCodeWords != meta.RoutingCodeWords):
		return nil, errors.Errorf("meta of an index scanned from memory: halo %t, routing code of %d bits and %d words, scan code of %d words",
			meta.Halo, meta.RoutingBits, meta.RoutingCodeWords, meta.ScanCodeWords)
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
	if meta.RoutingBits > 0 && !meta.ScanInMemory {
		d.st.scanQuantizer = scan
	}
	if meta.ScanInMemory {
		// The doc id of every slot, from the table of slots by doc id.
		d.docIDs = make([]uint64, meta.Vectors)
		held := 0
		for id, slot := range d.slotOf {
			if slot == diskNoSlot {
				continue
			}
			if int(slot) >= meta.Vectors {
				return nil, errors.Errorf("doc %d is in slot %d, the index has %d", id, slot, meta.Vectors)
			}
			d.docIDs[slot] = uint64(id)
			held++
		}
		if held != meta.Vectors {
			return nil, errors.Errorf("the table of slots holds %d docs, the index %d vectors", held, meta.Vectors)
		}
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

// residentMembers gives a posting its members from memory, in an index that
// keeps none on disk: their doc ids and their codes are the runs of the
// posting in the arrays by slot. Nothing is read and nothing is copied.
func (d *diskIndex) residentMembers(p uint32, dp *diskPosting) {
	words := d.meta.ScanCodeWords
	first, end := int(d.st.postings.start[p]), int(d.st.postings.start[p+1])
	dp.docIDs = d.docIDs[first:end:end]
	dp.codes = d.st.side.codes[first*words : end*words : end*words]
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
		dp.bytes, dp.took = [3]int{}, [3]time.Duration{}
		if d.meta.ScanInMemory {
			d.residentMembers(p, dp)
		} else {
			eg.Go(func() error {
				return timedRead(&dp.took[diskBlockMembers], func() error { return d.fetchMembers(p, dp) })
			})
		}
		eg.Go(func() error {
			return timedRead(&dp.took[diskBlockEdges], func() error { return d.fetchEdges(p, dp) })
		})
		if d.meta.Halo {
			eg.Go(func() error {
				return timedRead(&dp.took[diskBlockHalo], func() error { return d.fetchHalo(p, dp) })
			})
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

// search is the search of the index (Index.searchFrom) with the postings
// fetched from disk: the same entry, the same rounds and the same visits, in
// the same order. Every round is fetched whole before any of it is visited.
func (d *diskIndex) search(ctx context.Context, vector []float32, k int, stats *diskSearchStats,
) ([]uint64, []float32, error) {
	run := fetchedSearch{
		ix: d.ix, st: d.st, rounds: &d.rounds, fetch: d.fetch, halo: d.meta.Halo, haloWords: d.meta.RoutingCodeWords,
		noMembers: d.meta.ScanInMemory,
	}
	return run.run(ctx, vector, k, nil, stats)
}

// warmSegmentIndexes reads the index of every segment under dir, so that the
// system has it cached: the tree a lookup walks to find where a key lives,
// which the store maps and never loads. After the cache of the files has been
// dropped it leaves the data cold and the lookups warm, which is how a store
// that has been serving stands. It tells how many bytes it read.
func warmSegmentIndexes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".db" {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		// The header of a segment ends with where its index starts.
		header := make([]byte, 16)
		if _, err := file.ReadAt(header, 0); err != nil {
			return errors.Wrapf(err, "read the header of %s", filepath.Base(path))
		}
		start := int64(binary.LittleEndian.Uint64(header[8:]))
		if start < int64(len(header)) || start > info.Size() {
			return errors.Errorf("%s: the index starts at %d, the segment has %d bytes", filepath.Base(path), start, info.Size())
		}
		buf := make([]byte, info.Size()-start)
		if _, err := file.ReadAt(buf, start); err != nil && len(buf) > 0 {
			return errors.Wrapf(err, "read the index of %s", filepath.Base(path))
		}
		total += int64(len(buf))
		return nil
	})
	return total, err
}
