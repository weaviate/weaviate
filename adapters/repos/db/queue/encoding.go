package queue

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

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
