package memdb

import (
	"fmt"
	"strings"
	"testing"
)

func testDB(t *testing.T) *MemDB {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db
}

func TestTxn_Read_AbortCommit(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(false) // Readonly

	txn.Abort()
	txn.Abort()
	txn.Commit()
	txn.Commit()
}

func TestTxn_Write_AbortCommit(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true) // Write

	txn.Abort()
	txn.Abort()
	txn.Commit()
	txn.Commit()

	txn = db.Txn(true) // Write

	txn.Commit()
	txn.Commit()
	txn.Abort()
	txn.Abort()
}

func TestTxn_Insert_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := testObj()
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}
}

func TestTxn_InsertUpdate_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  "my-object",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}
}

func TestTxn_InsertUpdate_First_NonUnique(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "foo", obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  "my-object",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}

	// Lookup of the old value should fail
	raw, err = txn.First("main", "foo", obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_InsertUpdate_First_MultiIndex(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "qux", obj.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	raw, err = txn.First("main", "qux", obj.Qux[1])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  "my-object",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}

	raw, err = txn.First("main", "qux", obj2.Qux[1])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}

	// Lookup of the old value should fail
	raw, err = txn.First("main", "qux", obj.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	raw, err = txn.First("main", "qux", obj.Qux[1])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_First_NonUnique_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The first object has a unique secondary value
	raw, err := txn.First("main", "foo", obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}
}

func TestTxn_First_MultiIndex_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The first object has a unique secondary value
	raw, err := txn.First("main", "qux", obj.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}
}

func TestTxn_InsertDelete_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err := txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj1 {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Commit and start a new transaction
	txn.Commit()
	txn = db.Txn(true)

	// Delete obj1
	err = txn.Delete("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Lookup of the primary obj1 should fail
	raw, err = txn.First("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Lookup of the primary obj1 should fail
	raw, err = txn.First("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}
}

func TestTxn_InsertGet_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1"},
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *Txn) {
		// Attempt a row scan on the ID
		result, err := txn.Get("main", "id")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan on the ID with specific ID
		result, err = txn.Get("main", "id", obj1.ID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index
		result, err = txn.Get("main", "foo", obj1.Foo)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan multi index
		result, err = txn.Get("main", "qux", obj1.Qux[0])
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		result, err = txn.Get("main", "qux", obj2.Qux[1])
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
}

func TestTxn_DeleteAll_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc1"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Do a delete that doesn't hit any objects
	num, err := txn.DeleteAll("main", "id", "dogs")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 0 {
		t.Fatalf("bad: %d", num)
	}

	// Delete a specific ID
	num, err = txn.DeleteAll("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 1 {
		t.Fatalf("bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err := txn.First("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Delete an entire secondary range
	num, err = txn.DeleteAll("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 2 {
		t.Fatalf("Bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Insert some more
	err = txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Delete an entire multiindex range
	num, err = txn.DeleteAll("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 2 {
		t.Fatalf("Bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err = txn.First("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_DeleteAll_Prefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc1"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Delete a prefix
	num, err := txn.DeleteAll("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 3 {
		t.Fatalf("bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err := txn.First("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_DeletePrefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc1"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Lookup by qux field index
	iterator, err := txn.Get("main", "qux", "abc1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var objects []TestObject
	for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
		object := obj.(*TestObject)
		objects = append(objects, *object)
	}
	if len(objects) != 1 {
		t.Fatalf("Expected exactly one object")
	}
	expectedID := "my-object"
	if objects[0].ID != expectedID {
		t.Fatalf("Unexpected id, expected %v, but got %v", expectedID, objects[0].ID)
	}

	// Delete a prefix
	deleted, err := txn.DeletePrefix("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	if !deleted {
		t.Fatalf("Expected DeletePrefix to return true")
	}

	// Ensure we cannot lookup by id field index
	raw, err := txn.First("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if raw != nil {
		t.Fatalf("Unexpected value in tree: %#v", raw)
	}

	// Ensure we cannot lookup by qux or foo field indexes either anymore
	verifyNoResults(t, txn, "main", "qux", "abc1")
	verifyNoResults(t, txn, "main", "foo", "abc")
}
func verifyNoResults(t *testing.T, txn *Txn, table string, index string, value string) {
	iterator, err := txn.Get(table, index, value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if iterator != nil {
		next := iterator.Next()
		if next != nil {
			t.Fatalf("Unexpected values in tree, expected to be empty")
		}
	}
}

func TestTxn_InsertGet_Prefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "foobarbaz",
		Qux: []string{"foobarbaz", "fooqux"},
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "foozipzap",
		Qux: []string{"foozipzap"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *Txn) {
		// Attempt a row scan on the ID Prefix
		result, err := txn.Get("main", "id_prefix")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan on the ID with specific ID prefix
		result, err = txn.Get("main", "id_prefix", "my-c")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index
		result, err = txn.Get("main", "foo_prefix", "foo")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index, tigher prefix
		result, err = txn.Get("main", "foo_prefix", "foob")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan multiindex
		result, err = txn.Get("main", "qux_prefix", "foo")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		// second index entry for obj1 (fooqux)
		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan multiindex, tigher prefix
		result, err = txn.Get("main", "qux_prefix", "foob")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
}

// CustomIndex is a simple custom indexer that doesn't add any suffixes to its
// object keys; this is compatible with the LongestPrefixMatch algorithm.
type CustomIndex struct{}

// FromObject takes the Foo field of a TestObject and prepends a null.
func (*CustomIndex) FromObject(obj interface{}) (bool, []byte, error) {
	t, ok := obj.(*TestObject)
	if !ok {
		return false, nil, fmt.Errorf("not a test object")
	}

	// Prepend a null so we can address an empty Foo field.
	out := "\x00" + t.Foo
	return true, []byte(out), nil
}

// FromArgs always returns an error.
func (*CustomIndex) FromArgs(args ...interface{}) ([]byte, error) {
	return nil, fmt.Errorf("only prefix lookups are supported")
}

// Prefix from args takes the argument as a string and prepends a null.
func (*CustomIndex) PrefixFromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	arg, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("argument must be a string: %#v", args[0])
	}
	arg = "\x00" + arg
	return []byte(arg), nil
}

func TestTxn_InsertGet_LongestPrefix(t *testing.T) {
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"main": &TableSchema{
				Name: "main",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:   "id",
						Unique: true,
						Indexer: &StringFieldIndex{
							Field: "ID",
						},
					},
					"foo": &IndexSchema{
						Name:    "foo",
						Unique:  true,
						Indexer: &CustomIndex{},
					},
					"nope": &IndexSchema{
						Name:    "nope",
						Indexer: &CustomIndex{},
					},
				},
			},
		},
	}

	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "object1",
		Foo: "foo",
	}
	obj2 := &TestObject{
		ID:  "object2",
		Foo: "foozipzap",
	}
	obj3 := &TestObject{
		ID:  "object3",
		Foo: "",
	}

	err = txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *Txn) {
		raw, err := txn.LongestPrefix("main", "foo_prefix", "foo")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foobar")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozip")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozipza")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozipzap")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj2 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozipzapzone")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj2 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "funky")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj3 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj3 {
			t.Fatalf("bad: %#v", raw)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)

	// Try some disallowed index types.
	_, err = txn.LongestPrefix("main", "foo", "")
	if err == nil || !strings.Contains(err.Error(), "must use 'foo_prefix' on index") {
		t.Fatalf("bad: %v", err)
	}
	_, err = txn.LongestPrefix("main", "nope_prefix", "")
	if err == nil || !strings.Contains(err.Error(), "index 'nope_prefix' is not unique") {
		t.Fatalf("bad: %v", err)
	}
}

func TestTxn_Defer(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)
	res := ""

	// Defer a few functions
	txn.Defer(func() {
		res += "c"
	})
	txn.Defer(func() {
		res += "b"
	})
	txn.Defer(func() {
		res += "a"
	})

	// Commit the txn
	txn.Commit()

	// Check the result. All functions should have run, and should
	// have been executed in LIFO order.
	if res != "abc" {
		t.Fatalf("bad: %q", res)
	}
}

func TestTxn_Defer_Abort(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)
	res := ""

	// Defer a function
	txn.Defer(func() {
		res += "nope"
	})

	// Commit the txn
	txn.Abort()

	// Ensure deferred did not run
	if res != "" {
		t.Fatalf("bad: %q", res)
	}
}
