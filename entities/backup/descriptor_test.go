package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExcludeClasses(t *testing.T) {
	tests := []struct {
		in  BackupDescriptor
		xs  []string
		out []string
	}{
		{in: BackupDescriptor{}, xs: []string{}, out: []string{}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{}, out: []string{"a"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{"a"}, out: []string{}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}, {Name: "4"}}}, xs: []string{"2", "3"}, out: []string{"1", "4"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}}}, xs: []string{"1", "3"}, out: []string{"2"}},

		// {in: []BackupDescriptor{"1", "2", "3", "4"}, xs: []string{"2", "3"}, out: []string{"1", "4"}},
		// {in: []BackupDescriptor{"1", "2", "3"}, xs: []string{"1", "3"}, out: []string{"2"}},
	}
	for _, tc := range tests {
		tc.in.Exclude(tc.xs)
		lst := tc.in.List()
		assert.Equal(t, tc.out, lst)
	}
}

func TestIncludeClasses(t *testing.T) {
	tests := []struct {
		in  BackupDescriptor
		xs  []string
		out []string
	}{
		{in: BackupDescriptor{}, xs: []string{}, out: []string{}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{}, out: []string{"a"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{"a"}, out: []string{"a"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}, {Name: "4"}}}, xs: []string{"2", "3"}, out: []string{"2", "3"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}}}, xs: []string{"1", "3"}, out: []string{"1", "3"}},
	}
	for _, tc := range tests {
		tc.in.Include(tc.xs)
		lst := tc.in.List()
		assert.Equal(t, tc.out, lst)
	}
}

func TestAllExist(t *testing.T) {
	x := BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}
	if y := x.AllExists(nil); y != "" {
		t.Errorf("x.AllExists(nil) got=%v want=%v", y, "")
	}
	if y := x.AllExists([]string{"a"}); y != "" {
		t.Errorf("x.AllExists(['a']) got=%v want=%v", y, "")
	}
	if y := x.AllExists([]string{"b"}); y != "b" {
		t.Errorf("x.AllExists(['a']) got=%v want=%v", y, "b")
	}
}
