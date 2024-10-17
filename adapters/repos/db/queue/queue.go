//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package queue

import (
	"bufio"
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

type Queue struct {
	// Logger for the queue. Wrappers of this queue should use this logger.
	Logger logrus.FieldLogger
	// BeforeScheduleFn is a hook that is called before the queue is scheduled.
	BeforeScheduleFn func() (skip bool)

	scheduler    *Scheduler
	id           string
	path         string
	enc          *Encoder
	exec         TaskExecutor
	lastPushTime atomic.Pointer[time.Time]
	closed       atomic.Bool
}

func New(s *Scheduler, logger logrus.FieldLogger, id, path string, exec TaskExecutor) (*Queue, error) {
	logger = logger.WithField("queue_id", id)

	enc, err := NewEncoder(path, logger)
	if err != nil {
		return nil, err
	}

	q := Queue{
		Logger:    logger,
		scheduler: s,
		id:        id,
		path:      path,
		enc:       enc,
		exec:      exec,
	}

	s.RegisterQueue(&q)

	return &q, nil
}

// Close the queue, prevent further pushes and unregister it from the scheduler.
func (q *Queue) Close() error {
	if q.closed.Swap(true) {
		return errors.New("queue already closed")
	}

	q.scheduler.UnregisterQueue(q.id)

	err := q.enc.Close()
	if err != nil {
		return errors.Wrap(err, "failed to flush encoder")
	}

	return nil
}

func (q *Queue) ID() string {
	return q.id
}

func (q *Queue) Push(r *Record) error {
	if q.closed.Load() {
		return errors.New("queue closed")
	}

	now := time.Now()
	q.lastPushTime.Store(&now)

	return q.enc.Encode(r)
}

func (q *Queue) Flush() error {
	return q.enc.Flush()
}

func (q *Queue) DecodeTask(r *bufio.Reader) (*Task, error) {
	op, key, err := Decode(r)
	if err != nil {
		return nil, err
	}

	return &Task{
		Op:       op,
		IDs:      []uint64{key},
		executor: q.exec,
	}, nil
}

func (q *Queue) Size() int64 {
	return q.enc.RecordCount()
}

func (q *Queue) Pause() {
	q.scheduler.PauseQueue(q.id)
}

func (q *Queue) Resume() {
	q.scheduler.ResumeQueue(q.id)
}

func (q *Queue) Wait() {
	q.scheduler.Wait(q.id)
}

func (q *Queue) Drop() error {
	_ = q.Close()

	return q.enc.Drop()
}

func (q *Queue) BeforeSchedule() bool {
	if q.BeforeScheduleFn != nil {
		return q.BeforeScheduleFn()
	}

	return false
}

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type Record struct {
	*msgpack.Encoder
	buf *bytes.Buffer
}

func NewRecord() *Record {
	enc := msgpack.GetEncoder()
	buf := bufPool.Get().(*bytes.Buffer)
	enc.Reset(buf)

	return &Record{
		Encoder: enc,
		buf:     buf,
	}
}

func (r *Record) Release() {
	msgpack.PutEncoder(r.Encoder)
	r.buf.Reset()
	bufPool.Put(r.buf)
}

func (r *Record) Reset() {
	r.buf.Reset()
	r.Encoder.Reset(r.buf)
}

// shadow most msgpack methods with functions that don't return errors
func (r *Record) EncodeInt8(n int8) {
	r.Encoder.EncodeInt8(n)
}

func (r *Record) EncodeUint8(n uint8) {
	r.Encoder.EncodeUint8(n)
}

func (r *Record) EncodeUint16(n uint16) {
	r.Encoder.EncodeUint16(n)
}

func (r *Record) EncodeUint32(n uint32) {
	r.Encoder.EncodeUint32(n)
}

func (r *Record) EncodeUint64(n uint64) {
	r.Encoder.EncodeUint64(n)
}

func (r *Record) EncodeInt16(n int16) {
	r.Encoder.EncodeInt16(n)
}

func (r *Record) EncodeInt32(n int32) {
	r.Encoder.EncodeInt32(n)
}

func (r *Record) EncodeInt64(n int64) {
	r.Encoder.EncodeInt64(n)
}

func (r *Record) EncodeFloat32(n float32) {
	r.Encoder.EncodeFloat32(n)
}

func (r *Record) EncodeFloat64(n float64) {
	r.Encoder.EncodeFloat64(n)
}

func (r *Record) EncodeString(s string) {
	r.Encoder.EncodeString(s)
}

func (r *Record) EncodeBytes(b []byte) {
	r.Encoder.EncodeBytes(b)
}

func (r *Record) EncodeNil() {
	r.Encoder.EncodeNil()
}

func (r *Record) EncodeBool(b bool) {
	r.Encoder.EncodeBool(b)
}

func (r *Record) EncodeTime(t time.Time) {
	r.Encoder.EncodeTime(t)
}

func (r *Record) EncodeArrayLen(n int) {
	r.Encoder.EncodeArrayLen(n)
}

func (r *Record) EncodeMapLen(n int) {
	r.Encoder.EncodeMapLen(n)
}

type Decoder struct {
	*msgpack.Decoder
}

func NewDecoder(r io.Reader) *Decoder {
	dec := msgpack.GetDecoder()
	dec.Reset(r)

	return &Decoder{
		dec,
	}
}

func (d *Decoder) DecodeOp() (uint8, error) {
	n, err := d.DecodeInt8()
	return uint8(n), err
}

func (d *Decoder) DecodeFloat32Array() ([]float32, error) {
	n, err := d.DecodeArrayLen()
	if err != nil {
		return nil, err
	}

	vec := make([]float32, n)
	for i := 0; i < n; i++ {
		v, err := d.DecodeFloat32()
		if err != nil {
			return nil, err
		}

		vec[i] = v
	}

	return vec, nil
}
