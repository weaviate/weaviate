package inverted

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/filters"
)

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

// func combineSetChecksums(sets []*docPointers, operator filters.Operator) []byte {
// 	if len(sets) == 1 {
// 		return sets[0].checksum
// 	}

// 	total := make([]byte, 8*len(sets)+1) // one extra byte for operator encoding
// 	for i, set := range sets {
// 		copy(total[(i*8):(i+1)*8], set.checksum)
// 	}
// 	total[len(total)-1] = uint8(operator)

// 	newChecksum := crc64.Checksum(total, crc64.MakeTable(crc64.ISO))
// 	buf := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(buf, newChecksum)
// 	return buf
// }

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

// func checksumsIdentical(sets []*docPointers) bool {
// 	if len(sets) == 0 {
// 		return false
// 	}

// 	if len(sets) == 1 {
// 		return true
// 	}

// 	lastChecksum := sets[0].checksum
// 	for _, set := range sets {
// 		if !bytes.Equal(set.checksum, lastChecksum) {
// 			return false
// 		}
// 	}

// 	return true
// }

func checksumsIdenticalBM(docBitmaps []*docBitmap) bool {
	if len(docBitmaps) == 0 {
		return false
	}

	if len(docBitmaps) == 1 {
		return true
	}

	firstChecksum := docBitmaps[0].checksum
	for _, docBitmap := range docBitmaps {
		if !bytes.Equal(docBitmap.checksum, firstChecksum) {
			return false
		}
	}

	return true
}
