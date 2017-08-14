package memdb

import (
	"reflect"
	"testing"
	"time"
)

// Test that multiple concurrent transactions are isolated from each other
func TestTxn_Isolation(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(true)

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

	err := txn1.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Results should show up in this transaction
	raw, err := txn1.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Create a new transaction, current one is NOT committed
	txn2 := db.Txn(false)

	// Nothing should show up in this transaction
	raw, err = txn2.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Commit txn1, txn2 should still be isolated
	txn1.Commit()

	// Nothing should show up in this transaction
	raw, err = txn2.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Create a new txn
	txn3 := db.Txn(false)

	// Results should show up in this transaction
	raw, err = txn3.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil {
		t.Fatalf("bad: %#v", raw)
	}
}

// Test that an abort clears progress
func TestTxn_Abort(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(true)

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

	err := txn1.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Abort the txn
	txn1.Abort()
	txn1.Commit()

	// Create a new transaction
	txn2 := db.Txn(false)

	// Nothing should show up in this transaction
	raw, err := txn2.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestComplexDB(t *testing.T) {
	db := testComplexDB(t)
	testPopulateData(t, db)
	txn := db.Txn(false) // read only

	// Get using a full name
	raw, err := txn.First("people", "name", "Armon", "Dadgar")
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	// Get using a prefix
	raw, err = txn.First("people", "name_prefix", "Armon")
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	raw, err = txn.First("people", "id_prefix", raw.(*TestPerson).ID[:4])
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	// Get based on field set.
	result, err := txn.Get("people", "sibling", true)
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	exp := map[string]bool{"Alex": true, "Armon": true}
	act := make(map[string]bool, 2)
	for i := result.Next(); i != nil; i = result.Next() {
		p, ok := i.(*TestPerson)
		if !ok {
			t.Fatalf("should get person")
		}
		act[p.First] = true
	}

	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("Got %#v; want %#v", act, exp)
	}

	raw, err = txn.First("people", "sibling", false)
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}
	if raw.(*TestPerson).First != "Mitchell" {
		t.Fatalf("wrong person!")
	}

	raw, err = txn.First("people", "age", uint8(23))
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	person := raw.(*TestPerson)
	if person.First != "Alex" {
		t.Fatalf("wrong person!")
	}

	// Where in the world is mitchell hashimoto?
	raw, err = txn.First("people", "name_prefix", "Mitchell")
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	person = raw.(*TestPerson)
	if person.First != "Mitchell" {
		t.Fatalf("wrong person!")
	}

	raw, err = txn.First("visits", "id_prefix", person.ID)
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get visit")
	}

	visit := raw.(*TestVisit)

	raw, err = txn.First("places", "id", visit.Place)
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get place")
	}

	place := raw.(*TestPlace)
	if place.Name != "Maui" {
		t.Fatalf("bad place (but isn't anywhere else really?): %v", place)
	}
}

func TestWatchUpdate(t *testing.T) {
	db := testComplexDB(t)
	testPopulateData(t, db)
	txn := db.Txn(false) // read only

	watchSetIter := NewWatchSet()
	watchSetSpecific := NewWatchSet()
	watchSetPrefix := NewWatchSet()

	// Get using an iterator.
	iter, err := txn.Get("people", "name", "Armon", "Dadgar")
	noErr(t, err)
	watchSetIter.Add(iter.WatchCh())
	if raw := iter.Next(); raw == nil {
		t.Fatalf("should get person")
	}

	// Get using a full name.
	watch, raw, err := txn.FirstWatch("people", "name", "Armon", "Dadgar")
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}
	watchSetSpecific.Add(watch)

	// Get using a prefix.
	watch, raw, err = txn.FirstWatch("people", "name_prefix", "Armon")
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}
	watchSetPrefix.Add(watch)

	// Write to a snapshot.
	snap := db.Snapshot()
	txn2 := snap.Txn(true) // write
	noErr(t, txn2.Delete("people", raw))
	txn2.Commit()

	// None of the watches should trigger since we didn't alter the
	// primary.
	wait := 100 * time.Millisecond
	if timeout := watchSetIter.Watch(time.After(wait)); !timeout {
		t.Fatalf("should timeout")
	}
	if timeout := watchSetSpecific.Watch(time.After(wait)); !timeout {
		t.Fatalf("should timeout")
	}
	if timeout := watchSetPrefix.Watch(time.After(wait)); !timeout {
		t.Fatalf("should timeout")
	}

	// Write to the primary.
	txn3 := db.Txn(true) // write
	noErr(t, txn3.Delete("people", raw))
	txn3.Commit()

	// All three watches should trigger!
	wait = time.Second
	if timeout := watchSetIter.Watch(time.After(wait)); timeout {
		t.Fatalf("should not timeout")
	}
	if timeout := watchSetSpecific.Watch(time.After(wait)); timeout {
		t.Fatalf("should not timeout")
	}
	if timeout := watchSetPrefix.Watch(time.After(wait)); timeout {
		t.Fatalf("should not timeout")
	}
}

func testPopulateData(t *testing.T, db *MemDB) {
	// Start write txn
	txn := db.Txn(true)

	// Create some data
	person1 := testPerson()

	person2 := testPerson()
	person2.First = "Mitchell"
	person2.Last = "Hashimoto"
	person2.Age = 27

	person3 := testPerson()
	person3.First = "Alex"
	person3.Last = "Dadgar"
	person3.Age = 23

	person1.Sibling = person3
	person3.Sibling = person1

	place1 := testPlace()
	place2 := testPlace()
	place2.Name = "Maui"

	visit1 := &TestVisit{person1.ID, place1.ID}
	visit2 := &TestVisit{person2.ID, place2.ID}

	// Insert it all
	noErr(t, txn.Insert("people", person1))
	noErr(t, txn.Insert("people", person2))
	noErr(t, txn.Insert("people", person3))
	noErr(t, txn.Insert("places", place1))
	noErr(t, txn.Insert("places", place2))
	noErr(t, txn.Insert("visits", visit1))
	noErr(t, txn.Insert("visits", visit2))

	// Commit
	txn.Commit()
}

func noErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

type TestPerson struct {
	ID      string
	First   string
	Last    string
	Age     uint8
	Sibling *TestPerson
}

type TestPlace struct {
	ID   string
	Name string
}

type TestVisit struct {
	Person string
	Place  string
}

func testComplexSchema() *DBSchema {
	return &DBSchema{
		Tables: map[string]*TableSchema{
			"people": &TableSchema{
				Name: "people",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &UUIDFieldIndex{Field: "ID"},
					},
					"name": &IndexSchema{
						Name:   "name",
						Unique: true,
						Indexer: &CompoundIndex{
							Indexes: []Indexer{
								&StringFieldIndex{Field: "First"},
								&StringFieldIndex{Field: "Last"},
							},
						},
					},
					"age": &IndexSchema{
						Name:    "age",
						Unique:  false,
						Indexer: &UintFieldIndex{Field: "Age"},
					},
					"sibling": &IndexSchema{
						Name:    "sibling",
						Unique:  false,
						Indexer: &FieldSetIndex{Field: "Sibling"},
					},
				},
			},
			"places": &TableSchema{
				Name: "places",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &UUIDFieldIndex{Field: "ID"},
					},
					"name": &IndexSchema{
						Name:    "name",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "Name"},
					},
				},
			},
			"visits": &TableSchema{
				Name: "visits",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:   "id",
						Unique: true,
						Indexer: &CompoundIndex{
							Indexes: []Indexer{
								&UUIDFieldIndex{Field: "Person"},
								&UUIDFieldIndex{Field: "Place"},
							},
						},
					},
				},
			},
		},
	}
}

func testComplexDB(t *testing.T) *MemDB {
	db, err := NewMemDB(testComplexSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db
}

func testPerson() *TestPerson {
	_, uuid := generateUUID()
	obj := &TestPerson{
		ID:    uuid,
		First: "Armon",
		Last:  "Dadgar",
		Age:   26,
	}
	return obj
}

func testPlace() *TestPlace {
	_, uuid := generateUUID()
	obj := &TestPlace{
		ID:   uuid,
		Name: "HashiCorp",
	}
	return obj
}
