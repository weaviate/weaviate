package queue

import (
	"bytes"
	"io"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type MsgPackRecord struct {
	*msgpack.Encoder
	buf *bytes.Buffer
}

func NewRecord() *MsgPackRecord {
	enc := msgpack.GetEncoder()
	buf := bufPool.Get().(*bytes.Buffer)
	enc.Reset(buf)

	return &MsgPackRecord{
		Encoder: enc,
		buf:     buf,
	}
}

func (r *MsgPackRecord) Release() {
	msgpack.PutEncoder(r.Encoder)
	r.buf.Reset()
	bufPool.Put(r.buf)
}

func (r *MsgPackRecord) Reset() {
	r.buf.Reset()
	r.Encoder.Reset(r.buf)
}

func (r *MsgPackRecord) Bytes() []byte {
	return r.buf.Bytes()
}

func (r *MsgPackRecord) EncodeFloat32Array(a []float32) error {
	if err := r.EncodeArrayLen(len(a)); err != nil {
		return err
	}

	for _, v := range a {
		if err := r.EncodeFloat32(v); err != nil {
			return err
		}
	}

	return nil
}

type MsgPackDecoder struct {
	*msgpack.Decoder
}

func NewMsgPackDecoder() *MsgPackDecoder {
	dec := msgpack.GetDecoder()

	return &MsgPackDecoder{
		dec,
	}
}

func (d *MsgPackDecoder) Reset(r io.Reader) {
	d.Decoder.Reset(r)
}

func (d *MsgPackDecoder) DecodeFloat32Array() ([]float32, error) {
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
