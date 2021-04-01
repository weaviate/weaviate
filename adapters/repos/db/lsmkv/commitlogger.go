package lsmkv

import (
	"encoding/binary"
	"os"
)

type commitLogger struct {
	file *os.File
	path string
}

type CommitType uint16

const (
	CommitTypePut CommitType = iota
	CommitTypeSetTombstone
)

func newCommitLogger(path string) (*commitLogger, error) {
	out := &commitLogger{
		path: path + ".wal",
	}

	f, err := os.Create(out.path)
	if err != nil {
		return nil, err
	}

	out.file = f
	return out, nil
}

func (cl *commitLogger) put(key, value []byte) error {
	// TODO: do we need a timestamp? if so, does it need to be a vector clock?
	if err := binary.Write(cl.file, binary.LittleEndian, CommitTypePut); err != nil {
		return err
	}

	keyLen := uint32(len(key))
	if err := binary.Write(cl.file, binary.LittleEndian, &keyLen); err != nil {
		return err
	}

	if _, err := cl.file.Write(key); err != nil {
		return err
	}

	valueLen := uint32(len(value))
	if err := binary.Write(cl.file, binary.LittleEndian, &valueLen); err != nil {
		return err
	}

	if _, err := cl.file.Write(value); err != nil {
		return err
	}

	return nil
}

func (cl *commitLogger) setTombstone(key []byte) error {
	// TODO: do we need a timestamp? if so, does it need to be a vector clock?
	if err := binary.Write(cl.file, binary.LittleEndian, CommitTypeSetTombstone); err != nil {
		return err
	}

	keyLen := uint32(len(key))
	if err := binary.Write(cl.file, binary.LittleEndian, &keyLen); err != nil {
		return err
	}

	if _, err := cl.file.Write(key); err != nil {
		return err
	}

	return nil
}

func (cl *commitLogger) close() error {
	return cl.file.Close()
}

func (cl *commitLogger) delete() error {
	return os.Remove(cl.path)
}
