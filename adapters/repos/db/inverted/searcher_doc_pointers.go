//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc64"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func (fs *Searcher) docPointers(prop string, b *lsmkv.Bucket, limit int,
	pv *propValuePair, tolerateDuplicates bool) (docPointers, error) {
	if pv.operator == filters.OperatorWithinGeoRange {
		// geo props cannot be served by the inverted index and they require an
		// external index. So, instead of trying to serve this chunk of the filter
		// request internally, we can pass it to an external geo index
		return fs.docPointersGeo(pv)
	} else {
		// all other operators perform operations on the inverted index which we
		// can serve directly
		return fs.docPointersInverted(prop, b, limit, pv, tolerateDuplicates)
	}
}

func (fs *Searcher) docPointersInverted(prop string, b *lsmkv.Bucket, limit int,
	pv *propValuePair, tolerateDuplicates bool) (docPointers, error) {
	if pv.hasFrequency {
		return fs.docPointersInvertedFrequency(prop, b, limit, pv, tolerateDuplicates)
	}

	return fs.docPointersInvertedNoFrequency(prop, b, limit, pv, tolerateDuplicates)
}

func (fs *Searcher) docPointersInvertedNoFrequency(prop string, b *lsmkv.Bucket, limit int,
	pv *propValuePair, tolerateDuplicates bool) (docPointers, error) {
	rr := NewRowReader(b, pv.value, pv.operator, false)

	var pointers docPointers
	var hashes [][]byte

	if err := rr.Read(context.TODO(), func(k []byte, ids [][]byte) (bool, error) {
		currentDocIDs := make([]uint64, len(ids))
		for i, asBytes := range ids {
			currentDocIDs[i] = binary.LittleEndian.Uint64(asBytes)
		}

		pointers.count += uint64(len(ids))
		pointers.docIDs = append(pointers.docIDs, currentDocIDs...)

		hashBucket := fs.store.Bucket(helpers.HashBucketFromPropNameLSM(pv.prop))
		if hashBucket == nil {
			return false, errors.Errorf("no hash bucket for prop '%s' found", pv.prop)
		}

		currHash, err := hashBucket.Get(k)
		if err != nil {
			return false, errors.Wrap(err, "get hash")
		}

		hashes = append(hashes, currHash)
		if limit > 0 && pointers.count >= uint64(limit) {
			return false, nil
		}

		return true, nil
	}); err != nil {
		return pointers, errors.Wrap(err, "read row")
	}

	pointers.checksum = combineChecksums(hashes, pv.operator)
	if !tolerateDuplicates {
		pointers.removeDuplicates()
	}

	return pointers, nil
}

func (fs *Searcher) docPointersInvertedFrequency(prop string, b *lsmkv.Bucket, limit int,
	pv *propValuePair, tolerateDuplicates bool) (docPointers, error) {
	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false)

	var pointers docPointers
	var hashes [][]byte

	if err := rr.Read(context.TODO(), func(k []byte, pairs []lsmkv.MapPair) (bool, error) {
		currentDocIDs := make([]uint64, len(pairs))
		// beforePairs := time.Now()
		for i, pair := range pairs {
			// this entry has a frequency, but that's only used for bm25, not for
			// pure filtering, so we can ignore it here
			if fs.shardVersion < 2 {
				currentDocIDs[i] = binary.LittleEndian.Uint64(pair.Key)
			} else {
				currentDocIDs[i] = binary.BigEndian.Uint64(pair.Key)
			}
		}
		// fmt.Printf("loop through pairs took %s\n", time.Since(beforePairs))

		pointers.count += uint64(len(pairs))
		if len(pointers.docIDs) > 0 {
			pointers.docIDs = append(pointers.docIDs, currentDocIDs...)
		} else {
			pointers.docIDs = currentDocIDs
		}

		hashBucket := fs.store.Bucket(helpers.HashBucketFromPropNameLSM(pv.prop))
		if b == nil {
			return false, errors.Errorf("no hash bucket for prop '%s' found", pv.prop)
		}

		// use retrieved k instead of pv.value - they are typically the same, but
		// not on a like operator with wildcard where we only had a partial match
		currHash, err := hashBucket.Get(k)
		if err != nil {
			return false, errors.Wrap(err, "get hash")
		}

		hashes = append(hashes, currHash)
		if limit > 0 && pointers.count >= uint64(limit) {
			return false, nil
		}

		return true, nil
	}); err != nil {
		return pointers, errors.Wrap(err, "read row")
	}

	pointers.checksum = combineChecksums(hashes, pv.operator)

	if !tolerateDuplicates {
		pointers.removeDuplicates()
	}
	return pointers, nil
}

func (fs *Searcher) docPointersGeo(pv *propValuePair) (docPointers, error) {
	propIndex, ok := fs.propIndices.ByProp(pv.prop)
	out := docPointers{}
	if !ok {
		return out, nil
	}

	ctx := context.TODO() // TODO: pass through instead of spawning new
	res, err := propIndex.GeoIndex.WithinRange(ctx, *pv.valueGeoRange)
	if err != nil {
		return out, errors.Wrapf(err, "geo index range search on prop %q", pv.prop)
	}

	out.docIDs = res
	out.count = uint64(len(res))

	// we can not use the checksum in the same fashion as with the inverted
	// index, i.e. it can not prevent a search as the underlying index does not
	// have any understanding of checksums which could prevent such a read.
	// However, there is more use in the checksum: It can also be used in merging
	// searches (e.g. cond1 AND cond2). The merging operation itself is expensive
	// and cachable, therefore there is a lot of value in calculating and
	// returning a checksum - even for geoProps.
	chksum, err := docPointerChecksum(res)
	if err != nil {
		return out, errors.Wrap(err, "calculate checksum")
	}
	out.checksum = chksum

	return out, nil
}

// why is there a need to combine checksums prior to merging?
// on Operators GreaterThan (Equal) & LessThan (Equal), we don't just read a
// single row in the inverted index, but several (e.g. for greater than 5, we
// right read the row containing 5, 6, 7 and so on. Since a field contains just
// one value the docIDs are guaranteed to be unique, we can simply append them.
// But to be able to recognize this read operation further down the line (e.g.
// when merging independent filter) we need a new checksum describing exactly
// this request. Thus we simply treat the existing checksums as an input string
// (appended) and caculate a new one
func combineChecksums(checksums [][]byte, operator filters.Operator) []byte {
	if len(checksums) == 1 {
		return checksums[0]
	}

	total := make([]byte, len(checksums)*8+1) // one extra byte for operator encoding
	for i, chksum := range checksums {
		copy(total[(i*8):(i+1)*8], chksum)
	}
	total[len(total)-1] = uint8(operator)

	newChecksum := crc64.Checksum(total, crc64.MakeTable(crc64.ISO))
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, newChecksum)
	return buf
}

func combineSetChecksums(sets []*docPointers, operator filters.Operator) []byte {
	if len(sets) == 1 {
		return sets[0].checksum
	}

	total := make([]byte, 8*len(sets)+1) // one extra byte for operator encoding
	for i, set := range sets {
		copy(total[(i*8):(i+1)*8], set.checksum)
	}
	total[len(total)-1] = uint8(operator)

	newChecksum := crc64.Checksum(total, crc64.MakeTable(crc64.ISO))
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, newChecksum)
	return buf
}

// docPointerChecksum is a way to generate a checksum from an already "parsed"
// list of docIDs. This is untypical, as usually we can just use the raw binary
// value of the inverted row for a checksum. This also enables us to skip
// parsing and take a result from the cache. However, in external searches,
// such as with a geoProp per-property index, there is no raw row like there is
// on the inverted index. Instead the inner index alrady returns a list of
// docIDs
//
// This is probably not the most efficient way to do this, as we are doing an
// unnecessary binary.Write, just so we get a []byte which we can put into the
// crc64 function. But given how rare we expect this case to be in use cases,
// this seems like a good workaround for now. This might change.
func docPointerChecksum(pointers []uint64) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, len(pointers)*8))
	for _, p := range pointers {
		err := binary.Write(buf, binary.LittleEndian, uint64(p))
		if err != nil {
			return nil, errors.Wrap(err, "convert doc ids to little endian bytes")
		}
	}

	chksum := crc64.Checksum(buf.Bytes(), crc64.MakeTable(crc64.ISO))
	outBuf := bytes.NewBuffer(make([]byte, 0, 8))
	err := binary.Write(outBuf, binary.LittleEndian, &chksum)
	if err != nil {
		return nil, errors.Wrap(err, "convert checksum to bytes")
	}

	return buf.Bytes(), nil
}
