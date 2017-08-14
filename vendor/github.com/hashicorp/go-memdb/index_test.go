package memdb

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"testing"
)

type TestObject struct {
	ID       string
	Foo      string
	Bar      int
	Baz      string
	Bam      *bool
	Empty    string
	Qux      []string
	QuxEmpty []string
	Zod      map[string]string
	ZodEmpty map[string]string
	Uint     uint
	Uint8    uint8
	Uint16   uint16
	Uint32   uint32
	Uint64   uint64
}

func testObj() *TestObject {
	b := true
	obj := &TestObject{
		ID:  "my-cool-obj",
		Foo: "Testing",
		Bar: 42,
		Baz: "yep",
		Bam: &b,
		Qux: []string{"Test", "Test2"},
		Zod: map[string]string{
			"Role":          "Server",
			"instance_type": "m3.medium",
			"":              "asdf",
		},
		Uint:   uint(1),
		Uint8:  uint8(1<<8 - 1),
		Uint16: uint16(1<<16 - 1),
		Uint32: uint32(1<<32 - 1),
		Uint64: uint64(1<<64 - 1),
	}
	return obj
}

func TestStringFieldIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := StringFieldIndex{"Foo", false}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "Testing\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	lower := StringFieldIndex{"Foo", true}
	ok, val, err = lower.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "testing\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := StringFieldIndex{"NA", true}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := StringFieldIndex{"Empty", true}
	ok, val, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestStringFieldIndex_FromArgs(t *testing.T) {
	indexer := StringFieldIndex{"Foo", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}

	lower := StringFieldIndex{"Foo", true}
	val, err = lower.FromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}
}

func TestStringFieldIndex_PrefixFromArgs(t *testing.T) {
	indexer := StringFieldIndex{"Foo", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.PrefixFromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.PrefixFromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}

	lower := StringFieldIndex{"Foo", true}
	val, err = lower.PrefixFromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}
}

func TestStringSliceFieldIndex_FromObject(t *testing.T) {
	obj := testObj()

	indexer := StringSliceFieldIndex{"Qux", false}
	ok, vals, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatal("bad result length")
	}
	if string(vals[0]) != "Test\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "Test2\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	lower := StringSliceFieldIndex{"Qux", true}
	ok, vals, err = lower.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatal("bad result length")
	}
	if string(vals[0]) != "test\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "test2\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := StringSliceFieldIndex{"NA", true}
	ok, vals, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := StringSliceFieldIndex{"QuxEmpty", true}
	ok, vals, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestStringSliceFieldIndex_FromArgs(t *testing.T) {
	indexer := StringSliceFieldIndex{"Qux", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}

	lower := StringSliceFieldIndex{"Qux", true}
	val, err = lower.FromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}
}

func TestStringSliceFieldIndex_PrefixFromArgs(t *testing.T) {
	indexer := StringSliceFieldIndex{"Qux", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.PrefixFromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.PrefixFromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}

	lower := StringSliceFieldIndex{"Qux", true}
	val, err = lower.PrefixFromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}
}

func TestStringMapFieldIndex_FromObject(t *testing.T) {
	// Helper function to put the result in a deterministic order
	fromObjectSorted := func(index MultiIndexer, obj *TestObject) (bool, []string, error) {
		ok, v, err := index.FromObject(obj)
		var vals []string
		for _, s := range v {
			vals = append(vals, string(s))
		}
		sort.Strings(vals)
		return ok, vals, err
	}

	obj := testObj()

	indexer := StringMapFieldIndex{"Zod", false}
	ok, vals, err := fromObjectSorted(&indexer, obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatal("bad result length")
	}
	if string(vals[0]) != "Role\x00Server\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "instance_type\x00m3.medium\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	lower := StringMapFieldIndex{"Zod", true}
	ok, vals, err = fromObjectSorted(&lower, obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatal("bad result length")
	}
	if string(vals[0]) != "instance_type\x00m3.medium\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "role\x00server\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := StringMapFieldIndex{"NA", true}
	ok, _, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := StringMapFieldIndex{"ZodEmpty", true}
	ok, _, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestStringMapFieldIndex_FromArgs(t *testing.T) {
	indexer := StringMapFieldIndex{"Zod", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("Role", "Server")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "Role\x00Server\x00" {
		t.Fatalf("bad: %v", string(val))
	}

	lower := StringMapFieldIndex{"Zod", true}
	val, err = lower.FromArgs("Role", "Server")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "role\x00server\x00" {
		t.Fatalf("bad: %v", string(val))
	}
}

func TestUUIDFeldIndex_parseString(t *testing.T) {
	u := &UUIDFieldIndex{}
	_, err := u.parseString("invalid", true)
	if err == nil {
		t.Fatalf("should error")
	}

	buf, uuid := generateUUID()

	out, err := u.parseString(uuid, true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !bytes.Equal(out, buf) {
		t.Fatalf("bad: %#v %#v", out, buf)
	}

	_, err = u.parseString("1-2-3-4-5-6", false)
	if err == nil {
		t.Fatalf("should error")
	}

	// Parse an empty string.
	out, err = u.parseString("", false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected := []byte{}
	if !bytes.Equal(out, expected) {
		t.Fatalf("bad: %#v %#v", out, expected)
	}

	// Parse an odd length UUID.
	input := "f23"
	out, err = u.parseString(input, false)
	if err == nil {
		t.Fatalf("expect error")
	}

	// Parse an even length UUID with hyphen.
	input = "20d8c509-3940-"
	out, err = u.parseString(input, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected = []byte{0x20, 0xd8, 0xc5, 0x09, 0x39, 0x40}
	if !bytes.Equal(out, expected) {
		t.Fatalf("bad: %#v %#v", out, expected)
	}
}

func TestUUIDFieldIndex_FromObject(t *testing.T) {
	obj := testObj()
	uuidBuf, uuid := generateUUID()
	obj.Foo = uuid
	indexer := &UUIDFieldIndex{"Foo"}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := &UUIDFieldIndex{"NA"}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := &UUIDFieldIndex{"Empty"}
	ok, val, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestUUIDFieldIndex_FromArgs(t *testing.T) {
	indexer := &UUIDFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	uuidBuf, uuid := generateUUID()

	val, err := indexer.FromArgs(uuid)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}

	val, err = indexer.FromArgs(uuidBuf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}
}

func TestUUIDFieldIndex_PrefixFromArgs(t *testing.T) {
	indexer := UUIDFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.PrefixFromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	uuidBuf, uuid := generateUUID()

	// Test full length.
	val, err := indexer.PrefixFromArgs(uuid)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}

	val, err = indexer.PrefixFromArgs(uuidBuf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}

	// Test partial.
	val, err = indexer.PrefixFromArgs(uuid[:6])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf[:3], val) {
		t.Fatalf("PrefixFromArgs returned %#v;\nwant %#v", val, uuidBuf[:3])
	}

	val, err = indexer.PrefixFromArgs(uuidBuf[:9])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf[:9], val) {
		t.Fatalf("foo")
	}

}

func BenchmarkUUIDFieldIndex_parseString(b *testing.B) {
	_, uuid := generateUUID()
	indexer := &UUIDFieldIndex{}
	for i := 0; i < b.N; i++ {
		_, err := indexer.parseString(uuid, true)
		if err != nil {
			b.FailNow()
		}
	}
}

func generateUUID() ([]byte, string) {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	uuid := fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
	return buf, uuid
}

func TestUintFieldIndex_FromObject(t *testing.T) {
	obj := testObj()

	euint := make([]byte, 10)
	euint8 := make([]byte, 2)
	euint16 := make([]byte, 3)
	euint32 := make([]byte, 5)
	euint64 := make([]byte, 10)
	binary.PutUvarint(euint, uint64(obj.Uint))
	binary.PutUvarint(euint8, uint64(obj.Uint8))
	binary.PutUvarint(euint16, uint64(obj.Uint16))
	binary.PutUvarint(euint32, uint64(obj.Uint32))
	binary.PutUvarint(euint64, obj.Uint64)

	cases := []struct {
		Field         string
		Expected      []byte
		ErrorContains string
	}{
		{
			Field:    "Uint",
			Expected: euint,
		},
		{
			Field:    "Uint8",
			Expected: euint8,
		},
		{
			Field:    "Uint16",
			Expected: euint16,
		},
		{
			Field:    "Uint32",
			Expected: euint32,
		},
		{
			Field:    "Uint64",
			Expected: euint64,
		},
		{
			Field:         "UintGarbage",
			ErrorContains: "is invalid",
		},
		{
			Field:         "ID",
			ErrorContains: "want a uint",
		},
	}

	for _, c := range cases {
		t.Run(c.Field, func(t *testing.T) {
			indexer := UintFieldIndex{c.Field}
			ok, val, err := indexer.FromObject(obj)

			if err != nil {
				if ok {
					t.Fatalf("okay and error")
				}

				if c.ErrorContains != "" && strings.Contains(err.Error(), c.ErrorContains) {
					return
				} else {
					t.Fatalf("Unexpected error %v", err)
				}
			}

			if !ok {
				t.Fatalf("not okay and no error")
			}

			if !bytes.Equal(val, c.Expected) {
				t.Fatalf("bad: %#v %#v", val, c.Expected)
			}

		})
	}
}

func TestUintFieldIndex_FromArgs(t *testing.T) {
	indexer := UintFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(uint(1), uint(2))
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs("foo")
	if err == nil {
		t.Fatalf("should get err")
	}

	obj := testObj()
	euint := make([]byte, 10)
	euint8 := make([]byte, 2)
	euint16 := make([]byte, 3)
	euint32 := make([]byte, 5)
	euint64 := make([]byte, 10)
	binary.PutUvarint(euint, uint64(obj.Uint))
	binary.PutUvarint(euint8, uint64(obj.Uint8))
	binary.PutUvarint(euint16, uint64(obj.Uint16))
	binary.PutUvarint(euint32, uint64(obj.Uint32))
	binary.PutUvarint(euint64, obj.Uint64)

	val, err := indexer.FromArgs(obj.Uint)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint) {
		t.Fatalf("bad: %#v %#v", val, euint)
	}

	val, err = indexer.FromArgs(obj.Uint8)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint8) {
		t.Fatalf("bad: %#v %#v", val, euint8)
	}

	val, err = indexer.FromArgs(obj.Uint16)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint16) {
		t.Fatalf("bad: %#v %#v", val, euint16)
	}

	val, err = indexer.FromArgs(obj.Uint32)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint32) {
		t.Fatalf("bad: %#v %#v", val, euint32)
	}

	val, err = indexer.FromArgs(obj.Uint64)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint64) {
		t.Fatalf("bad: %#v %#v", val, euint64)
	}
}

func TestFieldSetIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := FieldSetIndex{"Bam"}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	emptyIndexer := FieldSetIndex{"Empty"}
	ok, val, err = emptyIndexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}

	setIndexer := FieldSetIndex{"Bar"}
	ok, val, err = setIndexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := FieldSetIndex{"NA"}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	obj.Bam = nil
	nilIndexer := FieldSetIndex{"Bam"}
	ok, val, err = nilIndexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}
}

func TestFieldSetIndex_FromArgs(t *testing.T) {
	indexer := FieldSetIndex{"Bam"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs(true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	val, err = indexer.FromArgs(false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}
}

var (
	// A conditional that checks if TestObject.Bar == 42
	conditional = func(obj interface{}) (bool, error) {
		test, ok := obj.(*TestObject)
		if !ok {
			return false, fmt.Errorf("Expect only TestObj types")
		}

		if test.Bar != 42 {
			return false, nil
		}

		return true, nil
	}
)

func TestConditionalIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := ConditionalIndex{conditional}
	obj.Bar = 42
	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	// Change the object so it should return false.
	obj.Bar = 2
	ok, val, err = indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}

	// Pass an invalid type.
	ok, val, err = indexer.FromObject(t)
	if err == nil {
		t.Fatalf("expected an error when passing invalid type")
	}
}

func TestConditionalIndex_FromArgs(t *testing.T) {
	indexer := ConditionalIndex{conditional}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs(true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	val, err = indexer.FromArgs(false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}
}

func TestCompoundIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := &CompoundIndex{
		Indexes: []Indexer{
			&StringFieldIndex{"ID", false},
			&StringFieldIndex{"Foo", false},
			&StringFieldIndex{"Baz", false},
		},
		AllowMissing: false,
	}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "my-cool-obj\x00Testing\x00yep\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	missing := &CompoundIndex{
		Indexes: []Indexer{
			&StringFieldIndex{"ID", false},
			&StringFieldIndex{"Foo", true},
			&StringFieldIndex{"Empty", false},
		},
		AllowMissing: true,
	}
	ok, val, err = missing.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "my-cool-obj\x00testing\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	// Test when missing not allowed
	missing.AllowMissing = false
	ok, _, err = missing.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not be okay")
	}
}

func TestCompoundIndex_FromArgs(t *testing.T) {
	indexer := &CompoundIndex{
		Indexes: []Indexer{
			&StringFieldIndex{"ID", false},
			&StringFieldIndex{"Foo", false},
			&StringFieldIndex{"Baz", false},
		},
		AllowMissing: false,
	}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42, 42, 42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("foo", "bar", "baz")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00bar\x00baz\x00" {
		t.Fatalf("bad: %s", val)
	}
}

func TestCompoundIndex_PrefixFromArgs(t *testing.T) {
	indexer := &CompoundIndex{
		Indexes: []Indexer{
			&UUIDFieldIndex{"ID"},
			&StringFieldIndex{"Foo", false},
			&StringFieldIndex{"Baz", false},
		},
		AllowMissing: false,
	}
	val, err := indexer.PrefixFromArgs()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 0 {
		t.Fatalf("bad: %s", val)
	}

	uuidBuf, uuid := generateUUID()
	val, err = indexer.PrefixFromArgs(uuid, "foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(val[:16], uuidBuf) {
		t.Fatalf("bad prefix")
	}
	if string(val[16:]) != "foo" {
		t.Fatalf("bad: %s", val)
	}

	val, err = indexer.PrefixFromArgs(uuid, "foo", "ba")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(val[:16], uuidBuf) {
		t.Fatalf("bad prefix")
	}
	if string(val[16:]) != "foo\x00ba" {
		t.Fatalf("bad: %s", val)
	}

	_, err = indexer.PrefixFromArgs(uuid, "foo", "bar", "nope")
	if err == nil {
		t.Fatalf("expected an error when passing too many arguments")
	}
}
