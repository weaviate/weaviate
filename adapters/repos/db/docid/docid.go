package docid

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

type Status uint8

const (
	Active Status = iota
	Deleted
)

type Lookup struct {
	DocID        uint32
	PointsTo     []byte
	Deleted      bool
	DeletionTime *time.Time // nil if Deleted==false
}

func AddLookupInTx(tx *bolt.Tx, info Lookup) error {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, info.DocID)
	key := keyBuf.Bytes()

	b := tx.Bucket(helpers.DocIDBucket)
	if b == nil {
		return fmt.Errorf("no index id bucket found")
	}

	value, err := info.MarshalBinary()
	if err != nil {
		return err
	}

	if err := b.Put(key, value); err != nil {
		return errors.Wrap(err, "store docId lookup info")
	}

	return nil
}

// doc id lookup structure is
//
// 0..1   uint8      status (0=active, 1=deleted)
// 1..16  [16]byte   marshalled UUID
// 8      uint64     (optional) deletion time stamp
func (l Lookup) MarshalBinary() ([]byte, error) {
	var deletionTimestamp int64
	status := Active
	if l.Deleted {
		if l.DeletionTime == nil {
			return nil, fmt.Errorf("lookup marked as deleted, but no deletion time set")
		}

		deletionTimestamp = l.DeletionTime.Unix()
		status = Deleted
	}

	if len(l.PointsTo) != 16 {
		return nil, fmt.Errorf("invalid length %d of id, must be 16 bytes",
			len(l.PointsTo))
	}

	buf := bytes.NewBuffer(make([]byte, 0, 25))
	if err := binary.Write(buf, binary.LittleEndian, status); err != nil {
		return nil, errors.Wrap(err, "write status")
	}

	if _, err := buf.Write(l.PointsTo); err != nil {
		return nil, errors.Wrap(err, "write target id")
	}

	if err := binary.Write(buf, binary.LittleEndian, deletionTimestamp); err != nil {
		return nil, errors.Wrap(err, "write deletion timestamp")
	}

	return buf.Bytes(), nil
}

func LookupFromBinary(in []byte) (Lookup, error) {
	var out Lookup

	if len(in) != 25 {
		return out, fmt.Errorf("expected doc id lookup row to have length 25: got %d",
			len(in))
	}

	r := bytes.NewReader(in)

	var status Status
	if err := binary.Read(r, binary.LittleEndian, &status); err != nil {
		return out, errors.Wrap(err, "unmarshal status")
	}

	out.PointsTo = make([]byte, 0, 16)
	if _, err := r.Read(out.PointsTo); err != nil {
		return out, errors.Wrap(err, "read id")
	}

	var delTime int64
	if err := binary.Read(r, binary.LittleEndian, &delTime); err != nil {
		return out, errors.Wrap(err, "unmarshal deletionTime")
	}

	if status == Deleted {
		out.Deleted = true
		parsed := time.Unix(delTime, 0)
		out.DeletionTime = &parsed
	}

	return out, nil
}

func MarkDeletedInTx(tx *bolt.Tx, docID uint32) error {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	key := keyBuf.Bytes()

	b := tx.Bucket(helpers.DocIDBucket)
	if b == nil {
		return fmt.Errorf("no index id bucket found")
	}

	v := b.Get(key)
	if v == nil {
		// already deleted, nothing to mark
		return nil
	}

	parsed, err := LookupFromBinary(v)
	if err != nil {
		return errors.Wrap(err, "unmarshal existing row")
	}

	parsed.Deleted = true
	now := time.Now()
	parsed.DeletionTime = &now

	bytes, err := parsed.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshal updated row")
	}

	if err := b.Put(key, bytes); err != nil {
		return errors.Wrap(err, "write row to bolt")
	}

	return nil
}

func RemoveInTx(tx *bolt.Tx, docID uint32) error {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	key := keyBuf.Bytes()

	b := tx.Bucket(helpers.DocIDBucket)
	if b == nil {
		return fmt.Errorf("no index id bucket found")
	}

	if err := b.Delete(key); err != nil {
		return errors.Wrap(err, "delete uuid for index id")
	}

	return nil
}
