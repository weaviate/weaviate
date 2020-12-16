package hnsw

import (
	"bytes"
	"math"

	"github.com/pkg/errors"
)

type bufferedLinksLogger struct {
	base *hnswCommitLogger
	buf  *bytes.Buffer
}

func (b *bufferedLinksLogger) ReplaceLinksAtLevel(nodeid uint64,
	level int, targets []uint64) error {
	b.base.writeCommitType(b.buf, ReplaceLinksAtLevel)
	b.base.writeUint64(b.buf, nodeid)
	b.base.writeUint16(b.buf, uint16(level))
	targetLength := len(targets)
	if targetLength > math.MaxUint16 {
		// TODO: investigate why we get such massive connections
		targetLength = math.MaxUint16
		b.base.logger.WithField("action", "hnsw_current_commit_log").
			WithField("id", b.base.id).
			WithField("original_length", len(targets)).
			WithField("maximum_length", targetLength).
			Warning("condensor length of connections would overflow uint16, cutting off")
	}
	b.base.writeUint16(b.buf, uint16(targetLength))
	b.base.writeUint64Slice(b.buf, targets[:targetLength])

	return nil
}

func (b *bufferedLinksLogger) Close() error {
	b.base.Lock()
	defer b.base.Unlock()

	_, err := b.base.logFile.Write(b.buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "flush link buffer to commit logger")
	}

	return nil
}
