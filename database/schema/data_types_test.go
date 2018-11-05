package schema

import (
	"github.com/creativesoftwarefdn/weaviate/models"
	"testing"
)

func TestDetectPrimitiveTypes(t *testing.T) {
	s := &Schema{}

	for _, type_ := range PrimitiveDataTypes {
		err, pdt := s.FindPropertyDataType([]string{string(type_)})
		if err != nil {
			t.Fatal(err)
		}

		if !pdt.IsPrimitive() {
			t.Fatal("not primitive")
		}

		if pdt.AsPrimitive() != type_ {
			t.Fatal("wrong value")
		}
	}
}

func TestNonExistingClassSingleRef(t *testing.T) {
	s := Empty()

	err, pdt := s.FindPropertyDataType([]string{"NonExistingClass"})

	if err == nil {
		t.Fatal("Should have error")
	}

	if pdt != nil {
		t.Fatal("Should return nil result")
	}
}

func TestExistingClassSingleRef(t *testing.T) {
	s := Empty()

	s.Actions.Classes = append(s.Actions.Classes, &models.SemanticSchemaClass{
		Class: "ExistingClass",
	})

	err, pdt := s.FindPropertyDataType([]string{"ExistingClass"})

	if err != nil {
		t.Fatal(err)
	}

	if !pdt.IsSingleRef() {
		t.Fatal("not single ref")
	}
}

func TestExistingClassMultipleRef(t *testing.T) {
	s := Empty()

	s.Actions.Classes = append(s.Actions.Classes, &models.SemanticSchemaClass{
		Class: "ExistingClass",
	})

	s.Actions.Classes = append(s.Actions.Classes, &models.SemanticSchemaClass{
		Class: "ExistingClassTwo",
	})

	err, pdt := s.FindPropertyDataType([]string{"ExistingClass", "ExistingClassTwo"})

	if err != nil {
		t.Fatal(err)
	}

	if !pdt.IsMultipleRef() {
		t.Fatal("not multiple ref")
	}
}
